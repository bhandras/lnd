package main

import (
	"bytes"
	"flag"
	"os"
	"text/template"
)

const (
	numberOfTypes uint32 = 300

	// customTypeStart defines the beginning of the custom TLV type range as
	// defined in BOLT-01.
	customTypeStart uint32 = 65536

	// pureTLVSecondSignedTypeRangeStart defines the first TLV type of the
	// second defined signed TLV range used in pure TLV messages.
	pureTLVSecondSignedTypeRangeStart uint32 = 1000000000

	// pureTLVSecondUnsignedTypeRangeStart defines the first TLV type of the
	// second defined unsigned TLV range used in pure TLV messages.
	pureTLVSecondUnsignedTypeRangeStart uint32 = 3000000000

	// inboundFeeType defines the TLV type used within the channel_update
	// message to indicate the inbound fee for a channel.
	inboundFeeType = 55555

	defaultOutputFile = "tlv_types_generated.go"
)

// typeMarkers defines any adhoc TLV types we want to generate.
var typeMarkers = map[uint32]struct{}{
	pureTLVSecondSignedTypeRangeStart:   {},
	pureTLVSecondUnsignedTypeRangeStart: {},
	inboundFeeType:                      {},
}

const typeCodeTemplate = `// Code generated by tlv/internal/gen; DO NOT EDIT.

package tlv

{{- range $index, $element := . }}

type tlvType{{ $index }} struct{}

func (t *tlvType{{ $index }}) TypeVal() Type {
	return {{ $index }}
}

func (t *tlvType{{ $index }}) tlv() {}

type TlvType{{ $index }} = *tlvType{{ $index }}
{{- end }}
`

func main() {
	// Create a slice of items that the template can range over.
	//
	// We'll generate 100 elements from the lower end of the TLV range
	// first.
	items := make(map[uint32]struct{})
	for i := uint32(0); i <= numberOfTypes; i++ {
		items[i] = struct{}{}
	}

	// With the lower end generated, we'll now generate 100 records in the
	// upper end of the range.
	for i := customTypeStart; i <= customTypeStart+numberOfTypes; i++ {
		items[i] = struct{}{}
	}

	// We'll also generate any marker types.
	for t := range typeMarkers {
		items[t] = struct{}{}
	}

	tpl, err := template.New("tlv").Parse(typeCodeTemplate)
	if err != nil {
		panic(err)
	}

	// Execute the template
	var out bytes.Buffer
	err = tpl.Execute(&out, items)
	if err != nil {
		panic(err)
	}

	outputFile := flag.String(
		"o", defaultOutputFile, "Output file for generated code",
	)
	flag.Parse()

	err = os.WriteFile(*outputFile, out.Bytes(), 0644)
	if err != nil {
		panic(err)
	}
}
