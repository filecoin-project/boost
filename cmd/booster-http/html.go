package main

import (
	"bytes"
	"html/template"
)

const idxTemplate = `
<html>
  <body>
    <h4>Booster HTTP Server</h4>
    Endpoints:
    <table>
      <tbody>
      {{ range . }}
        <tr>
          <td>
            {{ .Description }}
          </td>
          <td>
            {{ .Value }}
          </td>
        </tr>
      {{ end }}
      </tbody>
    </table>
  </body>
</html>
`

func parseTemplate(opts HttpServerOptions) string {
	type templateRow struct {
		Description string
		Value       template.HTML
	}
	endpoints := []templateRow{}

	if opts.ServePieces {
		endpoints = append(endpoints, templateRow{
			Description: "Download a raw piece by its piece CID",
			Value:       `<a href="/piece/bafySomePieceCid">/piece/&lt;piece cid&gt;</a>`,
		})
	}

	if opts.Blockstore != nil {
		endpoints = append(endpoints, templateRow{
			Description: "Download raw blocks or CAR files",
			Value:       `<a href="/ipfs/bafySomeBlockCid">/ipfs/&lt;block cid&gt;</a>`,
		})
	}

	t := template.Must(template.New("index.html").Parse(idxTemplate))
	var buff bytes.Buffer
	err := t.Execute(&buff, endpoints)
	if err != nil {
		panic(err)
	}
	return buff.String()
}
