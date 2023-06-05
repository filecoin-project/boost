package config

import (
	"bytes"
	"reflect"
	"regexp"
	"strings"
	"unicode"

	"github.com/BurntSushi/toml"
	"golang.org/x/xerrors"
)

// BoostConfigUpdate takes in a config and a default config and optionally comments out default values
func BoostConfigUpdate(cfgCur, cfgDef interface{}, comment bool) ([]byte, error) {
	var nodeStr, defStr string
	if cfgDef != nil {
		buf := new(bytes.Buffer)
		e := toml.NewEncoder(buf)
		if err := e.Encode(cfgDef); err != nil {
			return nil, xerrors.Errorf("encoding default config: %w", err)
		}

		defStr = buf.String()
	}

	{
		buf := new(bytes.Buffer)
		e := toml.NewEncoder(buf)
		if err := e.Encode(cfgCur); err != nil {
			return nil, xerrors.Errorf("encoding node config: %w", err)
		}

		nodeStr = buf.String()
	}

	if comment {
		// create a map of default lines, so we can comment those out later
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
		var outLines []string

		sectionRx := regexp.MustCompile(`\[(.+)]`)
		var section string

		for i, line := range nodeLines {
			// if this is a section, track it
			trimmed := strings.TrimSpace(line)
			if len(trimmed) > 0 {
				if trimmed[0] == '[' {
					m := sectionRx.FindSubmatch([]byte(trimmed))
					if len(m) != 2 {
						return nil, xerrors.Errorf("section didn't match (line %d)", i)
					}
					section = string(m[1])

					// never comment sections
					outLines = append(outLines, line)
					continue
				}
			}

			pad := strings.Repeat(" ", len(line)-len(strings.TrimLeftFunc(line, unicode.IsSpace)))

			// see if we have docs for this field
			{
				lf := strings.Fields(line)
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

					outLines = append(outLines, pad+"# env var: BOOST_"+strings.ToUpper(strings.ReplaceAll(section, ".", "_"))+"_"+strings.ToUpper(lf[0]))
				}
			}

			// if there is the same line in the default config, comment it out in output
			if _, found := defaults[strings.TrimSpace(nodeLines[i])]; (cfgDef == nil || found) && len(line) > 0 {
				line = pad + "#" + line[len(pad):]
			}
			outLines = append(outLines, line)
			if len(line) > 0 {
				outLines = append(outLines, "")
			}
		}

		nodeStr = strings.Join(outLines, "\n")
	}

	// sanity-check that the updated config parses the same way as the current one
	if cfgDef != nil {
		cfgUpdated, err := FromReader(strings.NewReader(nodeStr), cfgDef)
		if err != nil {
			return nil, xerrors.Errorf("parsing updated config: %w", err)
		}

		if !reflect.DeepEqual(cfgCur, cfgUpdated) {
			return nil, xerrors.Errorf("updated config didn't match current config")
		}
	}

	return []byte(nodeStr), nil
}
