package raft

type Log struct {
	Entries    []Entry
	FirstIndex int
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

func (log *Log) length() int {
	return len(log.Entries)
}

func (log *Log) lastEntry() Entry {
	return log.at(log.length() - 1 + log.FirstIndex)
}

func (log *Log) sliceToEnd(start int) []Entry {
	start -= log.FirstIndex
	return log.Entries[start:]
}

func (log *Log) findLastLogInTerm(x int) int {
	for i := log.lastEntry().Index; i > log.FirstIndex; i-- {
		term := log.at(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

func (log *Log) sliceFromStart(end int) []Entry {
	end -= log.FirstIndex
	return log.Entries[:end]
}

func (log *Log) at(index int) Entry {
	return log.Entries[index-log.FirstIndex]
}

func (log *Log) appendLog(entries ...Entry) {
	log.Entries = append(log.Entries, entries...)
}

func (log *Log) compactedTo(index int, term int) {
	suffix := make([]Entry, 0)
	suffixStart := index + 1
	if suffixStart <= log.lastEntry().Index {
		suffixStart = suffixStart - log.FirstIndex
		if suffixStart >= 0 {
			suffix = log.Entries[suffixStart:]
		}
	}

	log.Entries = append(make([]Entry, 1), suffix...)
	// set the dummy entry.
	log.Entries[0] = Entry{Index: index, Term: term}
	log.FirstIndex = index
}
