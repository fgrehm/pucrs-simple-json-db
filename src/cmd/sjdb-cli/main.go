package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	sjdb "simplejsondb"

	log "github.com/Sirupsen/logrus"
	"github.com/chzyer/readline"
)

func usage(w io.Writer) {
	io.WriteString(w, `
Available commands:
	[TODO] all <first-id> <count>
	insert <id> <json-string-template>
	bulk-insert <first-id> <last-id> <json-string-template>
	update <id> <new-json-string-template>
	find <id>
	delete <id>
	[TODO] search <attribute> <value>
	set-log-level <log-level>
	[TODO] inspect-block <data-block-id>
	show-tree
	exit
`[1:])
}

var completer = readline.NewPrefixCompleter(
	readline.PcItem("insert"),
	readline.PcItem("bulk-insert"),
	readline.PcItem("update"),
	readline.PcItem("find"),
	readline.PcItem("help"),
	readline.PcItem("delete"),
	readline.PcItem("search"),
	readline.PcItem("set-log-level",
		readline.PcItem("debug"),
		readline.PcItem("info"),
		readline.PcItem("warn"),
	),
	readline.PcItem("show-tree"),
	readline.PcItem("exit"),
)

func main() {
	log.SetLevel(log.WarnLevel)
	log.SetOutput(os.Stderr)

	l, err := readline.NewEx(&readline.Config{
		Prompt:       "\033[31m»\033[0m ",
		HistoryFile:  "/tmp/sjdb-readline.tmp",
		AutoComplete: completer,
	})
	if err != nil {
		panic(err)
	}
	db, err := sjdb.New("metadata-db.dat")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
		l.Close()
	}()

	log.SetOutput(l.Stderr())
	for {
		line, err := l.Readline()
		if err != nil {
			break
		}
		switch {
		case strings.HasPrefix(line, "set-log-level "):
			setLogLevel(strings.Trim(line[14:], " "))
		case strings.HasPrefix(line, "insert "):
			insert(db, l, line[7:])
		case strings.HasPrefix(line, "bulk-insert "):
			bulkInsert(db, l, line[12:])
		case strings.HasPrefix(line, "find "):
			find(db, line[5:])
		case strings.HasPrefix(line, "update "):
			update(db, l, line[7:])
		case strings.HasPrefix(line, "delete "):
			deleteRecord(db, line[7:])
		case strings.HasPrefix(strings.Trim(line, " "), "show-tree"):
			showTree(db)
		case line == "exit":
			goto exit
		case line == "help":
			usage(l.Stderr())
		case line == "":
		default:
			log.Error("Unknown command:", strconv.Quote(line))
		}
	}
exit:
}

func setLogLevel(level string) {
	switch level {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	default:
		fmt.Printf("Invalid log level: %#v", level)
	}
}

func insert(db sjdb.SimpleJSONDB, l *readline.Instance, args string) {
	idAndJson := strings.SplitN(args, " ", 2)
	if len(idAndJson) != 2 {
		usage(l.Stderr())
	}
	id, err := strconv.ParseUint(idAndJson[0], 10, 32)
	if err != nil {
		log.Error(err)
		return
	}
	if err = db.InsertRecord(uint32(id), idAndJson[1]); err != nil {
		log.Error(err)
	}
}

func update(db sjdb.SimpleJSONDB, l *readline.Instance, args string) {
	idAndJson := strings.SplitN(args, " ", 2)
	if len(idAndJson) != 2 {
		usage(l.Stderr())
	}
	id, err := strconv.ParseUint(idAndJson[0], 10, 32)
	if err != nil {
		log.Error(err)
		return
	}
	if err = db.UpdateRecord(uint32(id), idAndJson[1]); err != nil {
		log.Error(err)
	}
	log.Warn("Record updated")
}

func bulkInsert(db sjdb.SimpleJSONDB, l *readline.Instance, args string) {
	argsArr := strings.SplitN(args, " ", 3)
	if len(argsArr) != 3 {
		usage(l.Stderr())
		return
	}
	initialID, err := strconv.ParseUint(strings.Trim(argsArr[0], " "), 10, 32)
	if err != nil {
		log.Error(err)
		return
	}
	lastID, err := strconv.ParseUint(strings.Trim(argsArr[1], " "), 10, 32)
	if err != nil {
		log.Error(err)
		return
	}
	if initialID > lastID {
		log.Error("Invalid ID range provided")
		return
	}
	jsonStringTemplate := argsArr[2]
	for id := initialID; id <= lastID; id++ {
		log.Warnf("Inserting %v", id)
		if err = db.InsertRecord(uint32(id), jsonStringTemplate); err != nil {
			log.Error(err)
			return
		}
	}
	log.Warnf("%d records inserted", lastID-initialID+1)
}

func find(db sjdb.SimpleJSONDB, args string) {
	id, err := strconv.ParseUint(strings.Trim(args, " "), 10, 32)
	if err != nil {
		log.Error(err)
		return
	}
	record, err := db.FindRecord(uint32(id))
	if err != nil {
		log.Error(err)
		return
	}
	var out bytes.Buffer
	json.Indent(&out, record.Data, "", "  ")
	out.WriteString("\n")
	out.WriteTo(os.Stdout)
}

func deleteRecord(db sjdb.SimpleJSONDB, args string) {
	id, err := strconv.ParseUint(strings.Trim(args, " "), 10, 32)
	if err != nil {
		log.Error(err)
		return
	}
	if err = db.DeleteRecord(uint32(id)); err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Record %d deleted\n", id)
	log.Warnf("Record %d deleted", id)
}

func showTree(db sjdb.SimpleJSONDB) {
	println(db.DumpIndex())
}
