package database

import "strings"

/**
  Copyright © 2023 github.com/Allen9012 All rights reserved.
  @author: Allen
  @since: 2023/9/23
  @desc:
  @modified by:
**/

// GetUndoLogs return rollback commands
func (db *DB) GetUndoLogs(cmdLine [][]byte) []CmdLine {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	undo := cmd.undo
	if undo == nil {
		return nil
	}
	return undo(db, cmdLine[1:])
}

// GetRelatedKeys analysis related keys
func GetRelatedKeys(cmdLine [][]byte) ([]string, []string) {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil, nil
	}
	prepare := cmd.prepare
	if prepare == nil {
		return nil, nil
	}
	return prepare(cmdLine[1:])
}
