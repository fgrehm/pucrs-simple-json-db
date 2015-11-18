package cli

import (
	"bytes"
	"encoding/json"
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
	insert <id> <json-string>
	[TODO] bulk-insert <first-id> <last-id> <json-string-template>
	[TODO] update <id> <new-json-string>
	find <id>
	[TODO] delete <id>
	[TODO] search <attribute> <value>
	set-log-level <log-level>
	[TODO] inspect-block <data-block-id>
	[TODO] show-tree
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
	readline.PcItem("exit"),
)

func Run() {
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
			setLogLevel(line[14:])
		case strings.HasPrefix(line, "insert "):
			insert(db, l, line[7:])
		case strings.HasPrefix(line, "find "):
			find(db, l, line[5:])
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
		println("Invalid log level:", level)
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

func find(db sjdb.SimpleJSONDB, l *readline.Instance, args string) {
	id, err := strconv.ParseUint(args, 10, 32)
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
