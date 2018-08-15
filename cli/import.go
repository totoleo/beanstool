package cli

import (
	"fmt"
	"os"
	"bufio"
	"time"
	"math/rand"
	"github.com/kr/beanstalk"
)

type ImportCommand struct {
	Command
	Tube  string `short:"t" long:"tube" description:"tube to bury jobs in." required:"true"`
	Input string `short:"" long:"input" description:"input file" required:"true"`
}

func (c *ImportCommand) Execute(args []string) error {
	if err := c.Init(); err != nil {
		return err
	}
	if err := c.Import(); err != nil {
		return err
	}
	return nil
}

func (c *ImportCommand) Import() error {
	if c.Input == "" {
		fmt.Printf("Empty input file for tube %q.\n", c.Tube)
		return nil
	}

	fmt.Printf("Trying to import %q jobs from %s ...\n", c.Tube, c.Input)

	r, err := os.Open(c.Input)

	if err != nil {
		return err
	}
	defer r.Close()

	scanner := bufio.NewScanner(r)

	t := &beanstalk.Tube{Conn: c.conn, Name: c.Tube}

	for scanner.Scan() {
		delay := time.Duration(rand.Int() % 3600)
		line := scanner.Bytes()

		fmt.Println(string(line), delay*time.Second)

		_, err := t.Put(line, 1, delay*time.Second, 600*time.Second)
		if err != nil {
			return err
		}
	}
	return nil
}
