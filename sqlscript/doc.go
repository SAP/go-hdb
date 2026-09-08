// Package sqlscript provides functions to scan HDBSQL scripts with the help of bufio.Scanner.
// The statement separator (usually ';') is required between statements; it is not required
// after the final statement.
// This package is currently experimental and its public interface might be changed
// in an incompatible way at any time.
package sqlscript
