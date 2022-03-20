package main

type Metadata struct {
	value string
}

type RawMetadata []byte

func (m *Metadata) Marshal() (RawMetadata, error) {
	return []byte(m.value), nil
}

func (r RawMetadata) Unmarshal() (Metadata, error) {
	return Metadata{value: string(r)}, nil
}
