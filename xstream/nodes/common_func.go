package nodes

import "fmt"

func Broadcast(outputs map[string]chan<- interface{}, val interface{}) (err error) {
	for n, out := range outputs {
		select {
		case out <- val:
			//All ok
		default: //TODO channel full strategy?
			if err != nil {
				err = fmt.Errorf("%v;channel full for %s", err, n)
			} else {
				err = fmt.Errorf("channel full for %s", n)
			}
		}
	}
	return err
}


