package models

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/smartystreets/scanners/csv"
)

// Results wraps a set of PlantYear records
type Results struct {
	Data []PlantYear
}

// PlantYear is a single data point.
type PlantYear struct {
	Name          string  `csv:"Plant name" json:"plant_name"`
	Code          string  `csv:"DOE/EIA ORIS plant or facility code"`
	Year          string  `csv:"Data Year"`
	NumGenerators string  `csv:"Number of generators"`
	Fuel          string  `csv:"Plant primary fuel"`
	FuelCategory  string  `csv:"Plant primary fuel category"`
	UsesCoal      bool    `csv:"Flag indicating if the plant burned or generated any amount of coal"`
	Capacity      float32 `csv:"Plant nameplate capacity (MW)"`
	CO2Emissions  float32 `csv:"Plant annual CO2 emissions (tons)"`
}

func (py *PlantYear) GetID() string {
	return fmt.Sprintf("%s_%s", py.Year, py.Code)
}

// PlantScanner wraps the Smarty logic to convert from CSV into a struct.
type PlantScanner struct{ *csv.Scanner }

// NewPlantScanner is a Smarty helper function.
func NewPlantScanner(reader io.Reader) *PlantScanner {
	inner := csv.NewScanner(reader)
	inner.Scan() // skip the header!
	inner.Scan() // skip the subheader too!
	return &PlantScanner{Scanner: inner}
}

// Record overrides the default Smarty scanner logic.
func (ps *PlantScanner) Record() PlantYear {
	fields := ps.Scanner.Record()

	// Convert string into bool for UsesCoal field.
	var uc bool
	if fields[25] == "Yes" {
		uc = true
	}

	// Convert string to float for Capacity field.
	var capacity float32
	capString := strings.ReplaceAll(fields[27], ",", "")
	v, err := strconv.ParseFloat(capString, 32)
	if err == nil {
		capacity = float32(v)
	}

	// Convert string to float for CO2Emissions field.
	var co2 float32
	co2String := strings.ReplaceAll(fields[44], ",", "")
	v2, err := strconv.ParseFloat(co2String, 32)
	if err == nil {
		co2 = float32(v2)
	}

	return PlantYear{
		Name:          fields[3],
		Code:          fields[4],
		Year:          fields[1],
		NumGenerators: fields[22],
		Fuel:          fields[23],
		FuelCategory:  fields[24],
		UsesCoal:      uc,
		Capacity:      capacity,
		CO2Emissions:  co2,
	}
}
