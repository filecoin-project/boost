package react

import "embed"

// Embed the react build dir so that the web server can serve files from the
// build dir even if the boostd binary is moved somewhere else (because the
// files will be embedded in the binary)
//
//go:embed build
var BuildDir embed.FS
