package cli

import (
	"fmt"
	"github.com/kr/beanstalk"
	"os"
	"bufio"
)

type DumpCommand struct {
	Tube   string `short:"t" long:"tube" description:"tube to bury jobs in." required:"true"`
	State  string `short:"" long:"state" description:"peek from 'buried', 'ready' or 'delayed' queues." default:"buried"`
	Output string `short:"o" long:"output" description:"output file" required:"true"`
	Command
}

func (c *DumpCommand) Execute(args []string) error {
	if err := c.Init(); err != nil {
		return err
	}

	if err := c.Dump(); err != nil {
		return err
	}

	return nil
}

func (c *DumpCommand) Dump() error {
	if c.Output == "" {
		fmt.Printf("Empty output file for tube %q.\n", c.Tube)
		return nil
	}

	fmt.Printf("Trying to dump %q jobs to %s ...\n", c.Tube, c.Output)

	w, err := os.Create(c.Output)

	if err != nil {
		return err
	}
	defer w.Close()

	wb := bufio.NewWriter(w)
	defer wb.Flush()

	t := &beanstalk.Tube{Conn: c.conn, Name: c.Tube}
	var id uint64
	var body []byte

	for ; ; {
		switch c.State {
		case "buried":
			id, body, err = t.PeekBuried()
		case "ready":
			id, body, err = t.PeekReady()
		case "delayed":
			id, body, err = t.PeekDelayed()
		}

		if err != nil {
			return err
		}

		c.PrintJob(id, body)
		_, err = wb.Write(body)
		if err != nil {
			return err
		}
		err = wb.WriteByte('\n')
		if err != nil {
			return err
		}

		c.conn.Delete(id)
	}
	return nil
}
