package giza

type Metadata struct {
	Value string
}

type RawMetadata []byte

func (m *Metadata) Marshal() (RawMetadata, error) {
	return []byte(m.Value), nil
}

func (r RawMetadata) Unmarshal() (Metadata, error) {
	return Metadata{Value: string(r)}, nil
}
