package modbus

import (
        "fmt"
        "reflect"
        //"time"
        //"log"
        //"os"

        //"github.com/goburrow/modbus"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type query struct {
        Function string  `toml:"function"`
        DataSize *string `toml:"data_size"`
        DataType string  `toml:"data_type"`

        Register *byte   `toml:"register"`
        Address  *byte   `toml:"address"`

        Bit *byte `toml:"bit"`

        Quantity *uint16 `toml:"quantity"`

        Interval *internal.Duration `toml:"interval"`
}

func print(p interface{}) {
    v := reflect.ValueOf(p)
    if reflect.Ptr == v.Kind() {
        if !v.IsNil() {
            fmt.Println(*v.Pointer())
        }
    if nil == v {
        fmt.Println(v)
    } else {
        fmt.Println(*v)
    }
}

func (qry *query) print() {
    fmt.Println("Query")
    fmt.Println("Function:", qry.Function)
    fmt.Println("DataSize:"); print_ptr(qry.DataSize)
    fmt.Println("DataType:", qry.DataType)
    fmt.Println("Register:", *qry.Register)
    fmt.Println("Address: ", *qry.Address)
    fmt.Println("Bit:     ", *qry.Bit)
    fmt.Println("Quantity:", *qry.Quantity)
    fmt.Println("Interval:", *qry.Interval)
    fmt.Println()
}

type slave struct {
        Id      byte    `toml: id`
        Queries []query `toml:"query"`

        Interval *internal.Duration `toml:"default_interval"`
}

func (slv *slave) print() {
    fmt.Println("Slaves:")
    fmt.Println("Id:      ", slv.Id)
    fmt.Println("Interval:", slv.Interval)
    for i, qry := range slv.Queries {
        fmt.Print(i, " ")
        qry.print()
    }
    fmt.Println()
}

type Modbus struct {
        Mode        string `toml:"mode"`

        // TCP
        Host    *string `toml:"host"`
        Port    *uint16 `toml:"port"`

        // RTU || ASCII
        SerialPort *string `toml:"serial_port"`
        Baud       uint32  `toml:"baud"`

        Interval *internal.Duration `toml:"default_interval"`

        Slaves     []slave `toml:"slave"`
}

func (mb *Modbus) print() {
    fmt.Println("Modbus:")
    fmt.Println("Mode:      ", mb.Mode)
    fmt.Println("Host:      ", mb.Host)
    fmt.Println("Port:      ", mb.Port)
    fmt.Println("SerialPort:", mb.SerialPort)
    fmt.Println("Baud:      ", mb.Baud)
    fmt.Println("Interval:  ", mb.Interval)
    for i, slv := range mb.Slaves {
        fmt.Print(i, " ")
        slv.print()
    }
    fmt.Println()
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
  # mode    = "RTU"                # Required
  # serial_port = "/dev/ttyUSB0"   # Required
  # baud = 9600                    # Defaults to 9600

  ## ASCII Config
  # mode    = "ASCII"              # Required
  # serial_port = "/dev/ttyUSB0"   # Required
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
      register = 1


    [[inputs.modbus.slave.query]]
      function = "read_coils"
      register = 1
`
}

func (mb *Modbus) Gather(acc telegraf.Accumulator) error {
        return nil
}

func (mb *Modbus) Start(acc telegraf.Accumulator) error {
    mb.print()
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
