// +build tflite

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/emqx/kuiper/pkg/api"
	tflite "github.com/mattn/go-tflite"
	"github.com/nfnt/resize"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"path"
	"sort"
	"sync"
)

type labelImage struct {
	modelPath   string
	labelPath   string
	once        sync.Once
	interpreter *tflite.Interpreter
	labels      []string
}

func (f *labelImage) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("labelImage function only supports 1 parameter but got %d", len(args))
	}
	return nil
}

func (f *labelImage) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	arg0, ok := args[0].([]byte)
	if !ok {
		return fmt.Errorf("labelImage function parameter must be a bytea, but got %[1]T(%[1]v)", args[0]), false
	}
	img, _, err := image.Decode(bytes.NewReader(arg0))
	if err != nil {
		return err, false
	}
	var outerErr error
	f.once.Do(func() {
		ploc := path.Join(ctx.GetRootPath(), "etc", "functions")
		f.labels, err = loadLabels(path.Join(ploc, f.labelPath))
		if err != nil {
			outerErr = fmt.Errorf("fail to load labels: %s", err)
			return
		}

		model := tflite.NewModelFromFile(path.Join(ploc, f.modelPath))
		if model == nil {
			outerErr = fmt.Errorf("fail to load model: %s", err)
			return
		}
		defer model.Delete()

		options := tflite.NewInterpreterOptions()
		options.SetNumThread(4)
		options.SetErrorReporter(func(msg string, user_data interface{}) {
			fmt.Println(msg)
		}, nil)
		defer options.Delete()

		interpreter := tflite.NewInterpreter(model, options)
		if interpreter == nil {
			outerErr = fmt.Errorf("cannot create interpreter")
			return
		}
		status := interpreter.AllocateTensors()
		if status != tflite.OK {
			outerErr = fmt.Errorf("allocate failed")
			interpreter.Delete()
			return
		}

		f.interpreter = interpreter
		// TODO If created, the interpreter will be kept through the whole life of kuiper. Refactor this later.
		//defer interpreter.Delete()
	})

	if f.interpreter == nil {
		return fmt.Errorf("fail to load model %s %s", f.modelPath, outerErr), false
	}
	input := f.interpreter.GetInputTensor(0)
	wantedHeight := input.Dim(1)
	wantedWidth := input.Dim(2)
	wantedChannels := input.Dim(3)
	wantedType := input.Type()

	resized := resize.Resize(uint(wantedWidth), uint(wantedHeight), img, resize.NearestNeighbor)
	bounds := resized.Bounds()
	dx, dy := bounds.Dx(), bounds.Dy()

	if wantedType == tflite.UInt8 {
		bb := make([]byte, dx*dy*wantedChannels)
		for y := 0; y < dy; y++ {
			for x := 0; x < dx; x++ {
				col := resized.At(x, y)
				r, g, b, _ := col.RGBA()
				bb[(y*dx+x)*3+0] = byte(float64(r) / 255.0)
				bb[(y*dx+x)*3+1] = byte(float64(g) / 255.0)
				bb[(y*dx+x)*3+2] = byte(float64(b) / 255.0)
			}
		}
		input.CopyFromBuffer(bb)
	} else {
		return fmt.Errorf("is not wanted type"), false
	}

	status := f.interpreter.Invoke()
	if status != tflite.OK {
		return fmt.Errorf("invoke failed"), false
	}

	output := f.interpreter.GetOutputTensor(0)
	outputSize := output.Dim(output.NumDims() - 1)
	b := make([]byte, outputSize)
	type result struct {
		score float64
		index int
	}
	status = output.CopyToBuffer(&b[0])
	if status != tflite.OK {
		return fmt.Errorf("output failed"), false
	}
	var results []result
	for i := 0; i < outputSize; i++ {
		score := float64(b[i]) / 255.0
		if score < 0.2 {
			continue
		}
		results = append(results, result{score: score, index: i})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})
	// output is the biggest score labelImage
	if len(results) > 0 {
		return f.labels[results[0].index], true
	} else {
		return "", true
	}
}

func (f *labelImage) IsAggregate() bool {
	return false
}

func loadLabels(filename string) ([]string, error) {
	labels := []string{}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		labels = append(labels, scanner.Text())
	}
	return labels, nil
}

var LabelImage = labelImage{
	modelPath: "labelImage/mobilenet_quant_v1_224.tflite",
	labelPath: "labelImage/labels.txt",
}
