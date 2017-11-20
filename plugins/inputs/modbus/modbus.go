package modbus

import (
	"fmt"
	//"time"
	//"log"
	//"os"

	"github.com/AdamSLevy/modbus"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type query struct {
	Function string

	Address  uint16
	Quantity uint16

	Interval internal.Duration
}

type slave struct {
	Id      byte    `toml: id`
	Queries []query `toml:"query"`

	DefaultInterval internal.Duration
}

type Modbus struct {
	Mode    string
	Host    string
	Port    uint16
	Baud    uint
	Timeout internal.Duration

	DefaultInterval internal.Duration

	Slaves []slave `toml:"slave"`
}

func (mb *Modbus) SampleConfig() string {
	return `
  ## Connection Config
  ## Only use one of the following configs per [[ output.Modbus ]] section

  ## TCP Config
  mode = "TCP"             # Required
  host = "192.168.1.100"   # Required
  port = 502               # Defaults to 502 if omitted

  ## RTU Config
  # mode = "RTU"            # Required
  # host = "/dev/ttyUSB0"   # Required
  # baud = 9600             # Defaults to 9600

  ## ASCII Config
  # mode = "ASCII"              # Required
  # host = "/dev/ttyUSB0"   # Required
  # baud = 9600                    # Defaults to 9600

  ## The default query interval for all slaves
  # interval = "10s"    # Defaults to the global telegraf interval if not specified

  ## Slave Config
  [[inputs.modbus.slave]]
    id = 1

    ## The default query interval for this slave's queries
    # interval = "10s"    # Defaults to the global telegraf interval if not specified

    [[inputs.modbus.slave.query]]
      function = "read_coil"
      address = 1


    [[inputs.modbus.slave.query]]
      function = "read_coils"
      address = 1
`
}

func (mb *Modbus) Gather(acc telegraf.Accumulator) error {
	return nil
}

func (mb *Modbus) Start(acc telegraf.Accumulator) error {
	mode, ok := modbus.ModeByName[mb.Mode]
	if !ok {
		return fmt.Errorf("Unknown Mode: %v\n", mb.Mode)
	}
	switch mode {
	case modbus.ModeTCP:
		if 0 == mb.Port {
			mb.Port = 502
		}
		mb.Host = mb.Host + fmt.Sprintf(":%v", mb.Port)
	case modbus.ModeASCII:
		fallthrough
	case modbus.ModeRTU:
		if 0 == mb.Baud {
			mb.Baud = 9200
		}
	}

	cs := modbus.ConnectionSettings{
		Mode: mode,
		Host: mb.Host,
		Baud: mb.Baud,
	}
	fmt.Println("Connection Settings:", cs)
	return nil
}

func (mb *Modbus) Stop() {
	return
}

func (mb *Modbus) Description() string {
	return "Read multiple data points from multiple slaves over Modbus ASCII, RTU, " +
		"or TCP."
}

func init() {
	inputs.Add("modbus", func() telegraf.Input { return &Modbus{} })
}
