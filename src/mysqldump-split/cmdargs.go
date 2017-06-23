package main

import (
	"strings"
	"time"
)

// Options model for commandline arguments
type Options struct {
	HostName  string
	UserName  string
	Password  string
	Databases []string

	DatabaseRowCountTreshold int
	TableRowCountTreshold    int
	BatchSize                int
	ForceSplit               bool

	AdditionalMySQLDumpArgs string

	Verbosity              int
	MySQLDumpPath          string
	OutputDirectory        string
	DefaultsFile           string
	DefaultsProvidedByUser bool
	ExecutionStartDate     time.Time
}

// NewOptions returns a new Options instance.
func NewOptions(hostname string, username string, password string, databases string, databasetreshold int, tablethreshold int, batchsize int, forcesplit bool, additionals string, verbosity int, mysqldumppath string, outputDirectory string, defaultsFile string, defaultsProvidedByUser bool) *Options {
	databases = strings.Replace(databases, " ", "", -1)
	databases = strings.Replace(databases, " , ", ",", -1)
	databases = strings.Replace(databases, ", ", ",", -1)
	databases = strings.Replace(databases, " ,", ",", -1)
	dbs := strings.Split(databases, ",")
	dbs = removeDuplicates(dbs)

	return &Options{
		HostName:                 hostname,
		UserName:                 username,
		Password:                 password,
		Databases:                dbs,
		DatabaseRowCountTreshold: databasetreshold,
		TableRowCountTreshold:    tablethreshold,
		BatchSize:                batchsize,
		ForceSplit:               forcesplit,
		AdditionalMySQLDumpArgs:  additionals,
		Verbosity:                verbosity,
		MySQLDumpPath:            mysqldumppath,
		OutputDirectory:          outputDirectory,
		DefaultsFile:             defaultsFile,
		DefaultsProvidedByUser:   defaultsProvidedByUser,
		ExecutionStartDate:       time.Now(),
	}
}

func removeDuplicates(elements []string) []string {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	result := []string{}

	for v := range elements {
		if encountered[elements[v]] == true {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			encountered[elements[v]] = true
			// Append to result slice.
			result = append(result, elements[v])
		}
	}
	// Return the new slice.
	return result
}
