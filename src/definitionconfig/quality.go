// Package definitionconfig contains pure definition validation shared by owner tools and HTTP paths.
package definitionconfig

func VideoCodec(s string) bool { return s == "h264" || s == "h265" || s == "av1" }
func AudioCodec(s string) bool { return s == "aac" || s == "opus" }
func Container(s string) bool  { return s == "mp4" || s == "webm" || s == "mov" }
