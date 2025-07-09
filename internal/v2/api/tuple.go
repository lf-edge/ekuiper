package api

import "context"

type Tuple struct {
	Ctx          context.Context
	Columns      *Message
	AffiliateRow *Message
	Meta         map[string]string
}

func NewTupleFromData(stream string, data map[string]any) (*Tuple, error) {
	column, err := MapToMessage(stream, data)
	if err != nil {
		return nil, err
	}
	t := &Tuple{
		Ctx:          context.Background(),
		Columns:      column,
		Meta:         make(map[string]string),
		AffiliateRow: NewMessage(),
	}
	return t, nil
}

func NewTupleFromCtx(ctx context.Context, meta map[string]string) *Tuple {
	t := &Tuple{
		Ctx:          ctx,
		Columns:      NewMessage(),
		Meta:         meta,
		AffiliateRow: NewMessage(),
	}
	return t
}

func (t *Tuple) AppendAffiliateRow(key string, d *Datum) {
	t.AffiliateRow.Append("", key, d)
}

func (t *Tuple) AppendColumn(stream, key string, d *Datum) {
	t.Columns.Append(stream, key, d)
}

func (t *Tuple) ValueByKey(stream, s string) (*Datum, int, int, bool) {
	v, columnIndex, ok := t.Columns.ValueByKey(stream, s)
	if ok {
		return v, columnIndex, -1, true
	}
	v, affiliateRowIndex, ok := t.AffiliateRow.ValueByKey(stream, s)
	if ok {
		return v, -1, affiliateRowIndex, true
	}
	return nil, -1, -1, false
}

func (t *Tuple) ValueByColumnIndex(i int) (*Datum, string, string, bool) {
	return t.Columns.ValueByIndex(i)
}

func (t *Tuple) ValueByAffiliateRowIndex(i int) (*Datum, string, string, bool) {
	return t.AffiliateRow.ValueByIndex(i)
}

func (t *Tuple) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range t.Columns.ToMap() {
		result[k] = v
	}
	for k, v := range t.AffiliateRow.ToMap() {
		result[k] = v
	}
	return result
}

func FromMapToTuple(streamName string, m map[string]interface{}) (*Tuple, error) {
	cm, err := MapToMessage(streamName, m)
	if err != nil {
		return nil, err
	}
	return &Tuple{Columns: cm}, nil
}

type Message struct {
	Streams map[int]string
	Keys    []string
	Values  []*Datum
}

func NewMessage() *Message {
	return &Message{
		Streams: make(map[int]string),
		Keys:    make([]string, 0),
		Values:  make([]*Datum, 0),
	}
}

func (t *Message) Append(stream, k string, v *Datum) {
	t.Keys = append(t.Keys, k)
	t.Values = append(t.Values, v)
	if stream != "" {
		t.Streams[len(t.Keys)-1] = stream
	}
}

func (t *Message) ValueByKey(stream, key string) (*Datum, int, bool) {
	for index, name := range t.Keys {
		if name == key {
			if stream == "" {
				return t.Values[index], index, true
			}
			if stream == t.Streams[index] {
				return t.Values[index], index, true
			}
		}
	}
	return nil, 0, false
}

func (t *Message) ValueByIndex(i int) (*Datum, string, string, bool) {
	if i >= 0 && i < len(t.Values) {
		return t.Values[i], t.Streams[i], t.Keys[i], true
	}
	return nil, "", "", false
}

func (t *Message) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	for i, col := range t.Keys {
		if i < len(t.Values) {
			result[col] = datumToInterface(t.Values[i])
		}
	}
	return result
}

func (t *Message) ToDatum() *Datum {
	if t == nil || len(t.Keys) == 0 {
		return nil
	}
	d := &Datum{
		Kind:   MapVal,
		MapVal: make(map[string]*Datum),
	}
	for index, key := range t.Keys {
		d.MapVal[key] = t.Values[index]
	}
	return d
}

func MapToMessage(streamName string, m map[string]interface{}) (*Message, error) {
	columnNames := make([]string, 0, len(m))
	values := make([]*Datum, 0, len(m))
	for col, val := range m {
		columnNames = append(columnNames, col)
		v, err := interfaceToDatum(val)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	msg := &Message{Keys: columnNames, Values: values, Streams: make(map[int]string, len(m))}
	for index := 0; index < len(m); index++ {
		msg.Streams[index] = streamName
	}
	return msg, nil
}
