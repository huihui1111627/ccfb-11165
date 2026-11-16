// Package web embeds the management console assets.
package web

import "embed"

//go:embed static
var Files embed.FS
