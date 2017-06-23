package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"strings"

	"github.com/fatih/color"
)

const (
	// Info messages
	Info = 1 << iota // a == 1 (iota has been reset)

	// Warning Messages
	Warning = 1 << iota // b == 2

	// Error Messages
	Error = 1 << iota // c == 4
)

func main() {
	options := GetOptions()

	if !options.DefaultsProvidedByUser {
		createPwdFile(*options)
	}

	for _, db := range options.Databases {
		printMessage("Processing Database : "+db, options.Verbosity, Info)

		tables := GetTables(options.HostName, options.UserName, options.Password, db, options.Verbosity)
		totalRowCount := getTotalRowCount(tables)

		if !options.ForceSplit && totalRowCount <= options.DatabaseRowCountTreshold {
			// options.ForceSplit is false
			// and if total row count of a database is below defined threshold
			// then generate one file containing both schema and data

			printMessage(fmt.Sprintf("options.ForceSplit (%t) && totalRowCount (%d) <= options.DatabaseRowCountTreshold (%d)", options.ForceSplit, totalRowCount, options.DatabaseRowCountTreshold), options.Verbosity, Info)
			generateSingleFileBackup(*options, db)
		} else if options.ForceSplit && totalRowCount <= options.DatabaseRowCountTreshold {
			// options.ForceSplit is true
			// and if total row count of a database is below defined threshold
			// then generate two files one for schema, one for data

			generateSchemaBackup(*options, db)
			generateSingleFileDataBackup(*options, db)
		} else if totalRowCount > options.DatabaseRowCountTreshold {
			generateSchemaBackup(*options, db)

			for _, table := range tables {
				generateTableBackup(*options, db, table)
			}
		}

		printMessage("Processing done for database : "+db, options.Verbosity, Info)
	}

	if !options.DefaultsProvidedByUser {
		os.Remove(options.DefaultsFile)
	}
}

func createPwdFile(options Options) {

	f, err := os.OpenFile(options.DefaultsFile, os.O_CREATE, 0600)
	if err != nil {
		printMessage("Can not create password file : "+err.Error(), options.Verbosity, Error)
		os.Exit(2)
	}

	defer f.Close()

	text := `[mysqldump]
user=%s
password=%s`

	text = fmt.Sprintf(text, options.UserName, options.Password)
	if _, err = f.WriteString(text); err != nil {
		printMessage("Can not write to password file : "+err.Error(), options.Verbosity, Error)
		os.Exit(3)
	}
}

func generateTableBackup(options Options, db string, table Table) {
	printMessage("Generating table backup. Database : "+db+"\t\tTableName : "+table.TableName+"\t\tRowCount : "+strconv.Itoa(table.RowCount), options.Verbosity, Info)

	index := 1
	for counter := 0; counter <= table.RowCount; counter += options.BatchSize {

		var args []string
		args = append(args, fmt.Sprintf("--defaults-extra-file=%s", options.DefaultsFile))
		args = append(args, fmt.Sprintf("-h%s", options.HostName))
		args = append(args, fmt.Sprintf("-u%s", options.UserName))

		args = append(args, "--no-create-db")
		args = append(args, "--skip-triggers")
		args = append(args, "--no-create-info")

		if options.AdditionalMySQLDumpArgs != "" {
			args = append(args, strings.Split(options.AdditionalMySQLDumpArgs, " ")...)
		}

		// output-dir\\{DATABASE_NAME}\\{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.sql
		if runtime.GOOS == "windows" {
			timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02_15:04:05"), "-", "", -1), ":", "", -1)
			filename := path.Join(options.OutputDirectory, db, fmt.Sprintf("%s_%s%d_%s.sql", db, table.TableName, index, timestamp))
			_ = os.Mkdir(path.Dir(filename), os.ModePerm)
			filename = strings.Replace(filename, "/", "\\", -1)

			args = append(args, fmt.Sprintf("-r%s", filename))
		}

		args = append(args, fmt.Sprintf("--where=1=1 LIMIT %d, %d", counter, options.BatchSize))

		args = append(args, db)
		args = append(args, table.TableName)

		cmd := exec.Command(options.MySQLDumpPath, args...)
		cmdOut, _ := cmd.StdoutPipe()
		cmdErr, _ := cmd.StderrPipe()

		printMessage("mysqldump is being executed with parameters : "+strings.Join(cmd.Args, " "), options.Verbosity, Info)
		cmd.Start()

		output, _ := ioutil.ReadAll(cmdOut)
		err, _ := ioutil.ReadAll(cmdErr)
		cmd.Wait()

		printMessage("mysqldump output is : "+string(output), options.Verbosity, Info)
		if string(err) != "" {
			printMessage("mysqldump error is : "+string(err), options.Verbosity, Error)
			os.Exit(4)
		}

		index++
	}

	printMessage("Table backup successfull. Database : "+db+"\t\tTableName : "+table.TableName, options.Verbosity, Info)
}

func generateSchemaBackup(options Options, db string) {
	printMessage("Generating schema backup : "+db, options.Verbosity, Info)

	var args []string
	args = append(args, fmt.Sprintf("--defaults-extra-file=%s", options.DefaultsFile))
	args = append(args, fmt.Sprintf("-h%s", options.HostName))
	args = append(args, fmt.Sprintf("-u%s", options.UserName))

	args = append(args, "--no-data")

	if options.AdditionalMySQLDumpArgs != "" {
		args = append(args, strings.Split(options.AdditionalMySQLDumpArgs, " ")...)
	}

	// output-dir\\{DATABASE_NAME}\\{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.sql
	if runtime.GOOS == "windows" {
		timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02_15:04:05"), "-", "", -1), ":", "", -1)
		filename := path.Join(options.OutputDirectory, db, fmt.Sprintf("%s_%s_%s.sql", db, "SCHEMA", timestamp))
		_ = os.Mkdir(path.Dir(filename), os.ModePerm)
		filename = strings.Replace(filename, "/", "\\", -1)

		args = append(args, fmt.Sprintf("-r%s", filename))
	}
	args = append(args, db)

	printMessage("mysqldump is being executed with parameters : "+strings.Join(args, " "), options.Verbosity, Info)

	cmd := exec.Command(options.MySQLDumpPath, args...)
	cmdOut, _ := cmd.StdoutPipe()
	cmdErr, _ := cmd.StderrPipe()

	cmd.Start()

	output, _ := ioutil.ReadAll(cmdOut)
	err, _ := ioutil.ReadAll(cmdErr)
	cmd.Wait()

	printMessage("mysqldump output is : "+string(output), options.Verbosity, Info)
	if string(err) != "" {
		printMessage("mysqldump error is : "+string(err), options.Verbosity, Error)
		os.Exit(4)
	}

	printMessage("Schema backup successfull : "+db, options.Verbosity, Info)
}

func generateSingleFileDataBackup(options Options, db string) {
	printMessage("Generating single file data backup : "+db, options.Verbosity, Info)

	var args []string
	args = append(args, fmt.Sprintf("--defaults-extra-file=%s", options.DefaultsFile))
	args = append(args, fmt.Sprintf("-h%s", options.HostName))
	args = append(args, fmt.Sprintf("-u%s", options.UserName))

	args = append(args, "--no-create-db")
	args = append(args, "--skip-triggers")
	args = append(args, "--no-create-info")

	if options.AdditionalMySQLDumpArgs != "" {
		args = append(args, strings.Split(options.AdditionalMySQLDumpArgs, " ")...)
	}

	// output-dir\\{DATABASE_NAME}\\{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.sql
	if runtime.GOOS == "windows" {
		timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02_15:04:05"), "-", "", -1), ":", "", -1)
		filename := path.Join(options.OutputDirectory, db, fmt.Sprintf("%s_%s_%s.sql", db, "DATA", timestamp))
		_ = os.Mkdir(path.Dir(filename), os.ModePerm)
		filename = strings.Replace(filename, "/", "\\", -1)

		args = append(args, fmt.Sprintf("-r%s", filename))
	}
	args = append(args, db)

	printMessage("mysqldump is being executed with parameters : "+strings.Join(args, " "), options.Verbosity, Info)

	cmd := exec.Command(options.MySQLDumpPath, args...)
	cmdOut, _ := cmd.StdoutPipe()
	cmdErr, _ := cmd.StderrPipe()

	cmd.Start()

	output, _ := ioutil.ReadAll(cmdOut)
	err, _ := ioutil.ReadAll(cmdErr)
	cmd.Wait()

	printMessage("mysqldump output is : "+string(output), options.Verbosity, Info)
	if string(err) != "" {
		printMessage("mysqldump error is : "+string(err), options.Verbosity, Error)
		os.Exit(4)
	}

	printMessage("Single file data backup successfull : "+db, options.Verbosity, Info)
}

func generateSingleFileBackup(options Options, db string) {
	printMessage("Generating single file backup : "+db, options.Verbosity, Info)

	var args []string
	args = append(args, fmt.Sprintf("--defaults-extra-file=%s", options.DefaultsFile))
	args = append(args, fmt.Sprintf("-h%s", options.HostName))
	args = append(args, fmt.Sprintf("-u%s", options.UserName))
	if options.AdditionalMySQLDumpArgs != "" {
		args = append(args, strings.Split(options.AdditionalMySQLDumpArgs, " ")...)
	}

	// output-dir\\{DATABASE_NAME}\\{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.sql
	if runtime.GOOS == "windows" {
		timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02_15:04:05"), "-", "", -1), ":", "", -1)
		filename := path.Join(options.OutputDirectory, db, fmt.Sprintf("%s_%s_%s.sql", db, "ALL", timestamp))
		_ = os.Mkdir(path.Dir(filename), os.ModePerm)
		filename = strings.Replace(filename, "/", "\\", -1)

		args = append(args, fmt.Sprintf("-r%s", filename))
	}
	args = append(args, db)

	printMessage("mysqldump is being executed with parameters : "+strings.Join(args, " "), options.Verbosity, Info)

	cmd := exec.Command(options.MySQLDumpPath, args...)
	cmdOut, _ := cmd.StdoutPipe()
	cmdErr, _ := cmd.StderrPipe()

	cmd.Start()

	output, _ := ioutil.ReadAll(cmdOut)
	err, _ := ioutil.ReadAll(cmdErr)
	cmd.Wait()

	printMessage("mysqldump output is : "+string(output), options.Verbosity, Info)
	if string(err) != "" {
		printMessage("mysqldump error is : "+string(err), options.Verbosity, Error)
		os.Exit(4)
	}

	printMessage("Single file backup successfull : "+db, options.Verbosity, Info)
}

func getTotalRowCount(tables []Table) int {
	result := 0
	for _, table := range tables {
		result += table.RowCount
	}

	return result
}

// GetOptions creates Options type from Commandline arguments
func GetOptions() *Options {
	var hostname string
	flag.StringVar(&hostname, "hostname", "localhost", "Hostname of the mysql server to connect to")

	var username string
	flag.StringVar(&username, "username", "", "username of the mysql server to connect to")

	var password string
	flag.StringVar(&password, "password", "", "password of the mysql server to connect to")

	var databases string
	flag.StringVar(&databases, "databases", "", "list of databases as comma seperated values to dump")

	var dbthreshold int
	flag.IntVar(&dbthreshold, "dbthreshold", 10000000, "do not split mysqldumps, if total rowcount of tables in database is less than dbthreshold value for whole database")

	var tablethreshold int
	flag.IntVar(&tablethreshold, "tablethreshold", 5000000, "do not split mysqldumps, if rowcount of table is less than dbthreshold value for table")

	var batchsize int
	flag.IntVar(&batchsize, "batchsize", 1000000, "split mysqldumps in order to get each file contains batchsize number of records")

	var forcesplit bool
	flag.BoolVar(&forcesplit, "forcesplit", false, "split schema and data dumps even if total rowcount of tables in database is less than dbthreshold value. if false one dump file will be created")

	var additionals string
	flag.StringVar(&additionals, "additionals", "", "Additional parameters that will be appended to mysqldump command")

	var verbosity int
	flag.IntVar(&verbosity, "verbosity", 2, "0 = only errors, 1 = important things, 2 = all")

	var mysqldumppath string
	flag.StringVar(&mysqldumppath, "mysqldump-path", "", "Absolute path for mysqldump executable. Default value is \"/usr/bin/mysqldump\" for linux (without quotes), \"c:\\tools\\mysql\\current\\bin\\mysqldump.exe\" for windows (without quotes)")

	var outputdir string
	flag.StringVar(&outputdir, "output-dir", "", "Default is the value of os.Getwd(). The backup files will be placed to output-dir\\{DATABASE_NAME}\\{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.sql")

	var defaultsFile string
	flag.StringVar(&defaultsFile, "defaults-file", "", "Default is \"pwd.cnf\" located in output-dir.")

	var test bool
	flag.BoolVar(&test, "test", false, "test")

	flag.Parse()

	if mysqldumppath == "" {
		switch runtime.GOOS {
		case "windows":
			mysqldumppath = "c:\\tools\\mysql\\current\\bin\\mysqldump.exe"
		case "linux":
			mysqldumppath = "/usr/bin/mysqldump"
		}
	}

	if outputdir == "" {
		dir, err := os.Getwd()
		if err != nil {
			printMessage(err.Error(), verbosity, Error)
		}

		outputdir = dir
	}

	defaultsProvidedByUser := true
	if defaultsFile == "" {
		defaultsProvidedByUser = false
		defaultsFile = path.Join(outputdir, "pwd.cnf")
		if runtime.GOOS == "windows" {
			defaultsFile = strings.Replace(defaultsFile, "/", "\\", -1)
		}
	}

	if _, err := os.Stat(mysqldumppath); os.IsNotExist(err) {
		printMessage("mysqldump binary can not be found, please specify correct value for mysqldump-path parameter", verbosity, Error)
		os.Exit(1)
	}

	opts := NewOptions(hostname, username, password, databases, dbthreshold, tablethreshold, batchsize, forcesplit, additionals, verbosity, mysqldumppath, outputdir, defaultsFile, defaultsProvidedByUser)
	stropts, _ := json.MarshalIndent(opts, "", "\t")
	printMessage("Running with parameters", verbosity, Info)
	printMessage(string(stropts), verbosity, Info)
	printMessage("Running on operating system : "+runtime.GOOS, verbosity, Info)

	if test {
		cmd := exec.Command(opts.MySQLDumpPath,
			`--defaults-extra-file="E:\Temp\New Folder\pwd.cnf"`,
			`-h"10.2.1.181"`,
			`-u"selman"`,
			`--no-create-db`,
			`--skip-triggers`,
			`--no-create-info`,
			`--single-transaction`,
			`--skip-extended-insert`,
			`--quick`,
			`--skip-add-locks`,
			`--default-character-set=utf8`,
			`--compress`,
			`E:\Temp\New Folder\ftacs\ftacs_cpe_parameter2_20170622_080021.sql`,
			`--where="1=1 LIMIT 1000000, 1000000"`,
			`ftacs`,
			`cpe_parameter`)

		cmdOut, _ := cmd.StdoutPipe()
		cmdErr, _ := cmd.StderrPipe()

		cmd.Start()

		output, _ := ioutil.ReadAll(cmdOut)
		err, _ := ioutil.ReadAll(cmdErr)
		cmd.Wait()

		printMessage("mysqldump output is : "+string(output), opts.Verbosity, Info)
		if string(err) != "" {
			printMessage("mysqldump error is : "+string(err), opts.Verbosity, Error)
			os.Exit(4)
		}

		os.Exit(4)
	}

	return opts
}

func printMessage(message string, verbosity int, messageType int) {
	colors := map[int]color.Attribute{Info: color.FgGreen, Warning: color.FgHiYellow, Error: color.FgHiRed}

	if verbosity == 2 {
		color.Set(colors[messageType])
		fmt.Println(message)
		color.Unset()
	} else if verbosity == 1 && messageType > 1 {
		color.Set(colors[messageType])
		fmt.Println(message)
		color.Unset()
	} else if verbosity == 0 && messageType > 2 {
		color.Set(colors[messageType])
		fmt.Println(message)
		color.Unset()
	}
}

func checkErr(err error) {
	if err != nil {
		color.Set(color.FgHiRed)
		panic(err)
		color.Unset()
	}
}
