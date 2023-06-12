package config

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode"

	"github.com/BurntSushi/toml"
)

// ConfigUpdate takes in a config and a default config and optionally comments out default values
func ConfigUpdate(cfgCur, cfgDef interface{}, comment bool, diff bool) ([]byte, error) {
	var nodeStr, defStr string
	if cfgDef != nil {
		buf := new(bytes.Buffer)
		e := toml.NewEncoder(buf)
		if err := e.Encode(cfgDef); err != nil {
			return nil, fmt.Errorf("encoding default config: %w", err)
		}

		defStr = buf.String()
	}

	{
		buf := new(bytes.Buffer)
		e := toml.NewEncoder(buf)
		if err := e.Encode(cfgCur); err != nil {
			return nil, fmt.Errorf("encoding node config: %w", err)
		}

		nodeStr = buf.String()
	}

	var outLines []string

	if comment || diff {
		{
			// create a map of default lines so we can comment those out later
			defLines := strings.Split(defStr, "\n")
			defaults := map[string]struct{}{}
			for i := range defLines {
				l := strings.TrimSpace(defLines[i])
				if len(l) == 0 {
					continue
				}
				if l[0] == '#' || l[0] == '[' {
					continue
				}
				defaults[l] = struct{}{}
			}

			nodeLines := strings.Split(nodeStr, "\n")

			sectionRx := regexp.MustCompile(`\[(.+)]`)
			var section string

			for i, line := range nodeLines {
				// if this is a section, track it
				trimmed := strings.TrimSpace(line)
				if len(trimmed) > 0 {
					if trimmed[0] == '[' {
						m := sectionRx.FindSubmatch([]byte(trimmed))
						if len(m) != 2 {
							return nil, fmt.Errorf("section didn't match (line %d)", i)
						}
						section = string(m[1])

						// never comment sections
						outLines = append(outLines, line)
						continue
					}
				}

				pad := strings.Repeat(" ", len(line)-len(strings.TrimLeftFunc(line, unicode.IsSpace)))
				lf := strings.Fields(line)

				if diff {
					if len(lf) > 1 {
						if _, found := defaults[strings.TrimSpace(nodeLines[i])]; (cfgDef == nil || !found) && len(line) > 0 {
							if comment {
								doc := findDoc(cfgCur, section, lf[0])
								if doc != nil {
									// found docfield, emit doc comment
									if len(doc.Comment) > 0 {
										for _, docLine := range strings.Split(doc.Comment, "\n") {
											outLines = append(outLines, pad+"# "+docLine)
										}
										outLines = append(outLines, pad+"#")
									}

									outLines = append(outLines, pad+"# type: "+doc.Type)
								}
								outLines = append(outLines, line)
								if len(line) > 0 {
									outLines = append(outLines, "")
								}
							} else {
								outLines = append(outLines, line)
								if len(line) > 0 {
									outLines = append(outLines, "")
								}
							}

						}

					}

				} else {
					if len(lf) > 1 {
						doc := findDoc(cfgCur, section, lf[0])

						if doc != nil {
							// found docfield, emit doc comment
							if len(doc.Comment) > 0 {
								for _, docLine := range strings.Split(doc.Comment, "\n") {
									outLines = append(outLines, pad+"# "+docLine)
								}
								outLines = append(outLines, pad+"#")
							}

							outLines = append(outLines, pad+"# type: "+doc.Type)
						}
					}
					// if there is the same line in the default config, comment it out it output
					if _, found := defaults[strings.TrimSpace(nodeLines[i])]; (cfgDef == nil || found) && len(line) > 0 {
						line = pad + "#" + line[len(pad):]
					}
					outLines = append(outLines, line)
					if len(line) > 0 {
						outLines = append(outLines, "")
					}
				}
			}
		}

		if diff {
			outLines = removeLines(outLines)
		}

		nodeStr = strings.Join(outLines, "\n")
	}

	// sanity-check that the updated config parses the same way as the current one
	if cfgDef != nil {
		cfgUpdated, err := FromReader(strings.NewReader(nodeStr), cfgDef)
		if err != nil {
			return nil, fmt.Errorf("parsing updated config: %w", err)
		}

		if !reflect.DeepEqual(cfgCur, cfgUpdated) {
			return nil, fmt.Errorf("updated config didn't match current config\n%s\n%s", cfgCur, cfgUpdated)
		}
	}

	return []byte(nodeStr), nil
}

func removeLines(s []string) []string {
	var result []string
	var exit bool

	for i := 0; i < len(s); i++ {
		var savedLines []string
		if len(s[i]) > 0 {

			if string(s[i][0]) == "[" {
				savedLines = append(savedLines, s[i])

				for j := i + 1; j < len(s); j++ {
					if len(s[j]) > 0 && string(s[j][0]) == "[" {
						i = j - 1
						break
					}
					savedLines = append(savedLines, s[j])
				}
				if i+len(savedLines) >= len(s) {
					exit = true
				}
				if len(savedLines) < 2 {
					savedLines = nil
				} else {
					counter := 0
					for _, v := range savedLines {
						if len(v) > 0 && (string(strings.TrimSpace(v)[0]) == "[" || strings.TrimSpace(v) == "") {
							counter += 1
						}
					}
					if counter < len(savedLines) {
						res := cleanupSection(savedLines)
						result = append(result, res...)
					}
				}
				if exit {
					break
				}
			} else {
				result = append(result, s[i])
			}
		}
	}

	return result
}

func cleanupSection(sec []string) []string {
	var res []string
	res = append(res, sec[0])
	for i := 1; i < len(sec); i++ {
		var savedLines []string
		if len(sec[i]) > 0 && strings.TrimSpace(sec[i]) != "" {
			savedLines = append(savedLines, sec[i])
			for j := i + 1; j < len(sec); j++ {
				if len(sec[j]) > 0 && string(strings.TrimSpace(sec[j])[0]) == "[" {
					i = j - 1
					break
				}
				savedLines = append(savedLines, sec[j])
			}
			if len(savedLines) < 2 {
				savedLines = nil
			} else {
				res = append(res, savedLines...)
				if len(res) == len(sec) {
					return res
				}
			}
		}
	}

	if len(res) > 1 {
		return res
	}

	return nil
}
