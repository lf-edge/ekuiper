package main

import (
	"bufio"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/go-yaml/yaml"
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

var clientYaml = "client.yaml"

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

var Version string = "unknown"

func main() {
	app := cli.NewApp()
	app.Version = Version

	//nflag := []cli.Flag { cli.StringFlag{
	//		Name: "name, n",
	//		Usage: "the name of stream",
	//	}}

	b, err := common.LoadConf(clientYaml)
	if err != nil {
		common.Log.Fatal(err)
	}
	var cfg map[string]clientConf
	var config *clientConf
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		fmt.Printf("Failed to load config file with error %s.\n", err)
	} else {
		c, ok := cfg["basic"]
		if !ok {
			fmt.Printf("No basic config in client.yaml, will use the default configuration.\n")
		} else {
			config = &c
		}
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
			Usage:   "create stream $stream_name | create stream $stream_name -f $stream_def_file | create rule $rule_name $rule_json | create rule $rule_name -f $rule_def_file | create plugin $plugin_type $plugin_name $plugin_json | create plugin $plugin_type $plugin_name -f $plugin_def_file",

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
								args := &common.RuleDesc{rname, string(rule)}
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
							args := &common.RuleDesc{rname, rjson}
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
						args := &common.PluginDesc{
							RuleDesc: common.RuleDesc{
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
			},
		},
		{
			Name:    "describe",
			Aliases: []string{"describe"},
			Usage:   "describe stream $stream_name | describe rule $rule_name",
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
			},
		},

		{
			Name:    "drop",
			Aliases: []string{"drop"},
			Usage:   "drop stream $stream_name | drop rule $rule_name | drop plugin $plugin_type $plugin_name -r $restart",
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
					Usage: "drop plugin $plugin_type $plugin_name -r restart",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "restart, r",
							Usage: "restart kuiper after the action",
						},
					},
					Action: func(c *cli.Context) error {
						r := c.String("restart")
						if r != "true" && r != "false" {
							fmt.Printf("Expect r to be a boolean value.\n")
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
						args := &common.PluginDesc{
							RuleDesc: common.RuleDesc{
								Name: pname,
							},
							Type:    ptype,
							Restart: r == "true",
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
			},
		},

		{
			Name:    "show",
			Aliases: []string{"show"},
			Usage:   "show streams | show rules | show plugins $plugin_type",

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
