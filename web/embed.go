package web

import (
	"embed"
)

//go:generate make
//go:embed all:assets/web
var webFS embed.FS

//go:generate make
//go:embed all:assets/web_extensions
var webExtFS embed.FS
