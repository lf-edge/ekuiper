// Copyright 2021 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/model"
	"github.com/urfave/cli"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

type clientConf struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

const ClientYaml = "client.yaml"

func streamProcess(client *rpc.Client, args string) {
	var reply string
	if args == "" {
		args = strings.Join(os.Args[1:], " ")
	}
	err := client.Call("Server.Stream", args, &reply)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(reply)
	}
}

var (
	Version      = "unknown"
	LoadFileType = "relative"
)

func main() {
	conf.LoadFileType = LoadFileType
	app := cli.NewApp()
	app.Version = Version

	//nflag := []cli.Flag { cli.StringFlag{
	//		Name: "name, n",
	//		Usage: "the name of stream",
	//	}}

	var cfg map[string]clientConf
	err := conf.LoadConfigByName(ClientYaml, &cfg)
	if err != nil {
		conf.Log.Fatal(err)
		fmt.Printf("Failed to load config file with error %s.\n", err)
	}
	var config *clientConf
	c, ok := cfg["basic"]
	if !ok {
		fmt.Printf("No basic config in client.yaml, will use the default configuration.\n")
	} else {
		config = &c
	}

	if config == nil {
		config = &clientConf{
			Host: "127.0.0.1",
			Port: 20498,
		}
	}

	fmt.Printf("Connecting to %s:%d... \n", config.Host, config.Port)
	// Create a TCP connection to localhost on port 1234
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", config.Host, config.Port))
	if err != nil {
		fmt.Printf("Failed to connect the server, please start the server.\n")
		return
	}

	app.Commands = []cli.Command{
		{
			Name:    "query",
			Aliases: []string{"query"},
			Usage:   "query command line",
			Action: func(c *cli.Context) error {
				reader := bufio.NewReader(os.Stdin)
				var inputs []string
				ticker := time.NewTicker(time.Millisecond * 300)
				defer ticker.Stop()
				for {
					fmt.Print("kuiper > ")

					text, _ := reader.ReadString('\n')
					inputs = append(inputs, text)
					// convert CRLF to LF
					text = strings.Replace(text, "\n", "", -1)

					if strings.ToLower(text) == "quit" || strings.ToLower(text) == "exit" {
						break
					} else if strings.Trim(text, " ") == "" {
						continue
					} else {
						var reply string
						err := client.Call("Server.CreateQuery", text, &reply)
						if err != nil {
							fmt.Println(err)
							continue
						} else {
							fmt.Println(reply)
							go func() {
								for {
									<-ticker.C
									var result string
									e := client.Call("Server.GetQueryResult", "", &result)
									if e != nil {
										fmt.Println(e)
										fmt.Print("kuiper > ")
										return
									}
									if result != "" {
										fmt.Println(result)
									}
								}
							}()
						}
					}
				}
				return nil
			},
		},
		{
			Name:    "create",
			Aliases: []string{"create"},
			Usage:   "create stream $stream_name | create stream $stream_name -f $stream_def_file | create table $table_name | create table $table_name -f $table_def_file| create rule $rule_name $rule_json | create rule $rule_name -f $rule_def_file | create plugin $plugin_type $plugin_name $plugin_json | create plugin $plugin_type $plugin_name -f $plugin_def_file | create service $service_name $service_json ",

			Subcommands: []cli.Command{
				{
					Name:  "stream",
					Usage: "create stream $stream_name [-f stream_def_file]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:     "file, f",
							Usage:    "the location of stream definition file",
							FilePath: "/home/mystream.txt",
						},
					},
					Action: func(c *cli.Context) error {
						sfile := c.String("file")
						if sfile != "" {
							if stream, err := readDef(sfile, "stream"); err != nil {
								fmt.Printf("%s", err)
								return nil
							} else {
								args := strings.Join([]string{"CREATE STREAM ", string(stream)}, " ")
								streamProcess(client, args)
								return nil
							}
						} else {
							streamProcess(client, "")
							return nil
						}
					},
				},
				{
					Name:  "table",
					Usage: "create table $table_name [-f table_def_file]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:     "file, f",
							Usage:    "the location of table definition file",
							FilePath: "/home/mytable.txt",
						},
					},
					Action: func(c *cli.Context) error {
						sfile := c.String("file")
						if sfile != "" {
							if stream, err := readDef(sfile, "table"); err != nil {
								fmt.Printf("%s", err)
								return nil
							} else {
								args := strings.Join([]string{"CREATE TABLE ", string(stream)}, " ")
								streamProcess(client, args)
								return nil
							}
						} else {
							streamProcess(client, "")
							return nil
						}
					},
				},
				{
					Name:  "rule",
					Usage: "create rule $rule_name [$rule_json | -f rule_def_file]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:     "file, f",
							Usage:    "the location of rule definition file",
							FilePath: "/home/myrule.txt",
						},
					},
					Action: func(c *cli.Context) error {
						sfile := c.String("file")
						if sfile != "" {
							if rule, err := readDef(sfile, "rule"); err != nil {
								fmt.Printf("%s", err)
								return nil
							} else {
								if len(c.Args()) != 1 {
									fmt.Printf("Expect rule name.\n")
									return nil
								}
								rname := c.Args()[0]
								var reply string
								args := &model.RPCArgDesc{Name: rname, Json: string(rule)}
								err = client.Call("Server.CreateRule", args, &reply)
								if err != nil {
									fmt.Println(err)
								} else {
									fmt.Println(reply)
								}
							}
							return nil
						} else {
							if len(c.Args()) != 2 {
								fmt.Printf("Expect rule name and json.\nBut found %d args:%s.\n", len(c.Args()), c.Args())
								return nil
							}
							rname := c.Args()[0]
							rjson := c.Args()[1]
							var reply string
							args := &model.RPCArgDesc{Name: rname, Json: rjson}
							err = client.Call("Server.CreateRule", args, &reply)
							if err != nil {
								fmt.Println(err)
							} else {
								fmt.Println(reply)
							}
							return nil
						}
					},
				},
				{
					Name:  "plugin",
					Usage: "create plugin $plugin_type $plugin_name [$plugin_json | -f plugin_def_file]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:     "file, f",
							Usage:    "the location of plugin definition file",
							FilePath: "/home/myplugin.txt",
						},
					},
					Action: func(c *cli.Context) error {
						if len(c.Args()) < 2 {
							fmt.Printf("Expect plugin type and name.\n")
							return nil
						}
						ptype, err := getPluginType(c.Args()[0])
						if err != nil {
							fmt.Printf("%s\n", err)
							return nil
						}
						pname := c.Args()[1]
						sfile := c.String("file")
						args := &model.PluginDesc{
							RPCArgDesc: model.RPCArgDesc{
								Name: pname,
							},
							Type: ptype,
						}
						if sfile != "" {
							if len(c.Args()) != 2 {
								fmt.Printf("Expect plugin type, name.\nBut found %d args:%s.\n", len(c.Args()), c.Args())
								return nil
							}
							if p, err := readDef(sfile, "plugin"); err != nil {
								fmt.Printf("%s", err)
								return nil
							} else {
								args.Json = string(p)
							}
						} else {
							if len(c.Args()) != 3 {
								fmt.Printf("Expect plugin type, name and json.\nBut found %d args:%s.\n", len(c.Args()), c.Args())
								return nil
							}
							args.Json = c.Args()[2]
						}
						var reply string
						err = client.Call("Server.CreatePlugin", args, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
				{
					Name:  "service",
					Usage: "create service $service_name $service_json",
					Action: func(c *cli.Context) error {
						if len(c.Args()) < 2 {
							fmt.Printf("Expect service name and json.\n")
							return nil
						}
						var reply string
						err = client.Call("Server.CreateService", &model.RPCArgDesc{
							Name: c.Args()[0],
							Json: c.Args()[1],
						}, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
		{
			Name:    "describe",
			Aliases: []string{"describe"},
			Usage:   "describe stream $stream_name | describe table $table_name | describe rule $rule_name | describe plugin $plugin_type $plugin_name | describe udf $udf_name | describe service $service_name | describe service_func $service_func_name",
			Subcommands: []cli.Command{
				{
					Name:  "stream",
					Usage: "describe stream $stream_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						streamProcess(client, "")
						return nil
					},
				},
				{
					Name:  "table",
					Usage: "describe table $table_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						streamProcess(client, "")
						return nil
					},
				},
				{
					Name:  "rule",
					Usage: "describe rule $rule_name",
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.DescRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
				{
					Name:  "plugin",
					Usage: "describe plugin $plugin_type $plugin_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						ptype, err := getPluginType(c.Args()[0])
						if err != nil {
							fmt.Printf("%s\n", err)
							return nil
						}
						if len(c.Args()) != 2 {
							fmt.Printf("Expect plugin name.\n")
							return nil
						}
						pname := c.Args()[1]
						args := &model.PluginDesc{
							RPCArgDesc: model.RPCArgDesc{
								Name: pname,
							},
							Type: ptype,
						}

						var reply string
						err = client.Call("Server.DescPlugin", args, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
				{
					Name:  "udf",
					Usage: "describe udf $udf_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect udf name.\n")
							return nil
						}
						pname := c.Args()[0]
						var reply string
						err = client.Call("Server.DescUdf", pname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
				{
					Name:  "service",
					Usage: "describe service $service_name",
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect service name.\n")
							return nil
						}
						name := c.Args()[0]
						var reply string
						err = client.Call("Server.DescService", name, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
				{
					Name:  "service_func",
					Usage: "describe service_func $service_func_name",
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect service func name.\n")
							return nil
						}
						name := c.Args()[0]
						var reply string
						err = client.Call("Server.DescServiceFunc", name, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},

		{
			Name:    "drop",
			Aliases: []string{"drop"},
			Usage:   "drop stream $stream_name | drop table $table_name |drop rule $rule_name | drop plugin $plugin_type $plugin_name -r $stop | drop service $service_name",
			Subcommands: []cli.Command{
				{
					Name:  "stream",
					Usage: "drop stream $stream_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						streamProcess(client, "")
						return nil
					},
				},
				{
					Name:  "table",
					Usage: "drop table $table_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						streamProcess(client, "")
						return nil
					},
				},
				{
					Name:  "rule",
					Usage: "drop rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.DropRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
				{
					Name:  "plugin",
					Usage: "drop plugin $plugin_type $plugin_name -s stop",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "stop, s",
							Usage: "stop kuiper after the action",
						},
					},
					Action: func(c *cli.Context) error {
						r := c.String("stop")
						if r != "true" && r != "false" {
							fmt.Printf("Expect s flag to be a boolean value.\n")
							return nil
						}
						if len(c.Args()) < 2 || len(c.Args()) > 3 {
							fmt.Printf("Expect plugin type and name.\n")
							return nil
						}
						ptype, err := getPluginType(c.Args()[0])
						if err != nil {
							fmt.Printf("%s\n", err)
							return nil
						}
						pname := c.Args()[1]
						args := &model.PluginDesc{
							RPCArgDesc: model.RPCArgDesc{
								Name: pname,
							},
							Type: ptype,
							Stop: r == "true",
						}

						var reply string
						err = client.Call("Server.DropPlugin", args, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
				{
					Name:  "service",
					Usage: "drop service $service_name",
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect service name.\n")
							return nil
						}
						name := c.Args()[0]
						var reply string
						err = client.Call("Server.DropService", name, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},

		{
			Name:    "show",
			Aliases: []string{"show"},
			Usage:   "show streams | show tables | show rules | show plugins $plugin_type | show services | show service_funcs",

			Subcommands: []cli.Command{
				{
					Name:  "streams",
					Usage: "show streams",
					Action: func(c *cli.Context) error {
						streamProcess(client, "")
						return nil
					},
				},
				{
					Name:  "tables",
					Usage: "show tables",
					Action: func(c *cli.Context) error {
						streamProcess(client, "")
						return nil
					},
				},
				{
					Name:  "rules",
					Usage: "show rules",
					Action: func(c *cli.Context) error {
						var reply string
						err = client.Call("Server.ShowRules", 0, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
				{
					Name:  "plugins",
					Usage: "show plugins $plugin_type",
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect plugin type.\n")
							return nil
						}
						ptype, err := getPluginType(c.Args()[0])
						if err != nil {
							fmt.Printf("%s\n", err)
							return nil
						}
						var reply string
						err = client.Call("Server.ShowPlugins", ptype, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
				{
					Name:  "udfs",
					Usage: "show udfs",
					Action: func(c *cli.Context) error {
						var reply string
						err = client.Call("Server.ShowUdfs", 0, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				}, {
					Name:  "services",
					Usage: "show services",
					Action: func(c *cli.Context) error {
						var reply string
						err = client.Call("Server.ShowServices", 0, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				}, {
					Name:  "service_funcs",
					Usage: "show service_funcs",
					Action: func(c *cli.Context) error {
						var reply string
						err = client.Call("Server.ShowServiceFuncs", 0, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},

		{
			Name:    "getstatus",
			Aliases: []string{"getstatus"},
			Usage:   "getstatus rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "rule",
					Usage: "getstatus rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.GetStatusRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
		{
			Name:    "gettopo",
			Aliases: []string{"gettopo"},
			Usage:   "gettopo rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "rule",
					Usage: "getstopo rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.GetTopoRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
		{
			Name:    "start",
			Aliases: []string{"start"},
			Usage:   "start rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "rule",
					Usage: "start rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.StartRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
		{
			Name:    "stop",
			Aliases: []string{"stop"},
			Usage:   "stop rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "rule",
					Usage: "stop rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.StopRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
		{
			Name:    "restart",
			Aliases: []string{"restart"},
			Usage:   "restart rule $rule_name",
			Subcommands: []cli.Command{
				{
					Name:  "rule",
					Usage: "restart rule $rule_name",
					//Flags: nflag,
					Action: func(c *cli.Context) error {
						if len(c.Args()) != 1 {
							fmt.Printf("Expect rule name.\n")
							return nil
						}
						rname := c.Args()[0]
						var reply string
						err = client.Call("Server.RestartRule", rname, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
		{
			Name:    "register",
			Aliases: []string{"register"},
			Usage:   "register plugin function $plugin_name [$plugin_json | -f plugin_def_file]",
			Subcommands: []cli.Command{
				{
					Name:  "plugin",
					Usage: "register plugin $plugin_type $plugin_name [$plugin_json | -f plugin_def_file]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:     "file, f",
							Usage:    "the location of plugin functions definition file",
							FilePath: "/home/myplugin.txt",
						},
					},
					Action: func(c *cli.Context) error {
						if len(c.Args()) < 2 {
							fmt.Printf("Expect plugin type and name.\n")
							return nil
						}
						ptype := c.Args()[0]
						if strings.ToLower(ptype) != "function" {
							fmt.Printf("Plugin type must be function.\n")
							return nil
						}
						pname := c.Args()[1]
						sfile := c.String("file")
						args := &model.PluginDesc{
							RPCArgDesc: model.RPCArgDesc{
								Name: pname,
							},
						}
						if sfile != "" {
							if len(c.Args()) != 2 {
								fmt.Printf("Expect plugin type, name.\nBut found %d args:%s.\n", len(c.Args()), c.Args())
								return nil
							}
							if p, err := readDef(sfile, "plugin"); err != nil {
								fmt.Printf("%s", err)
								return nil
							} else {
								args.Json = string(p)
							}
						} else {
							if len(c.Args()) != 3 {
								fmt.Printf("Expect plugin type, name and json.\nBut found %d args:%s.\n", len(c.Args()), c.Args())
								return nil
							}
							args.Json = c.Args()[2]
						}
						var reply string
						err = client.Call("Server.RegisterPlugin", args, &reply)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(reply)
						}
						return nil
					},
				},
			},
		},
	}

	app.Name = "Kuiper"
	app.Usage = "The command line tool for EMQ X Kuiper."

	app.Action = func(c *cli.Context) error {
		cli.ShowSubcommandHelp(c)
		//cli.ShowVersion(c)

		return nil
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	err = app.Run(os.Args)
	if err != nil {
		fmt.Printf("%v", err)
	}
}

func getPluginType(arg string) (ptype int, err error) {
	switch arg {
	case "source":
		ptype = 0
	case "sink":
		ptype = 1
	case "function":
		ptype = 2
	default:
		err = fmt.Errorf("Invalid plugin type %s, should be \"source\", \"sink\" or \"function\".\n", arg)
	}
	return
}

func readDef(sfile string, t string) ([]byte, error) {
	if _, err := os.Stat(sfile); os.IsNotExist(err) {
		return nil, fmt.Errorf("The specified %s defenition file %s is not existed.\n", t, sfile)
	}
	fmt.Printf("Creating a new %s from file %s.\n", t, sfile)
	if rule, err := ioutil.ReadFile(sfile); err != nil {
		return nil, fmt.Errorf("Failed to read from %s definition file %s.\n", t, sfile)
	} else {
		return rule, nil
	}
}
