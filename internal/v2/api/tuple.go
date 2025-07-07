package api

import "context"

type Tuple struct {
	ctx          context.Context
	StreamName   string
	Columns      *Message
	AffiliateRow *Message
	Meta         map[string]string
}

func (t *Tuple) AppendAffiliateRow(key string, d *Datum) {
	t.AffiliateRow.Append(key, d)
}

func (t *Tuple) ValueByKey(s string) (*Datum, int, int, bool) {
	v, columnIndex, ok := t.Columns.ValueByKey(s)
	if ok {
		return v, columnIndex, -1, true
	}
	v, affiliateRowIndex, ok := t.AffiliateRow.ValueByKey(s)
	if ok {
		return v, -1, affiliateRowIndex, true
	}
	return nil, -1, -1, false
}

func (t *Tuple) ValueByColumnIndex(i int) (*Datum, string, bool) {
	return t.Columns.ValueByIndex(i)
}

func (t *Tuple) ValueByAffiliateRowIndex(i int) (*Datum, string, bool) {
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

func FromMapToTuple(m map[string]interface{}) (*Tuple, error) {
	cm, err := MapToMessage(m)
	if err != nil {
		return nil, err
	}
	return &Tuple{Columns: cm}, nil
}

type Message struct {
	Keys   []string
	Values []*Datum
}

func (t *Message) Append(k string, v *Datum) {
	t.Keys = append(t.Keys, k)
	t.Values = append(t.Values, v)
}

func (t *Message) ValueByKey(s string) (*Datum, int, bool) {
	for index, name := range t.Keys {
		if name == s {
			return t.Values[index], index, true
		}
	}
	return nil, 0, false
}

func (t *Message) ValueByIndex(i int) (*Datum, string, bool) {
	if i >= 0 && i < len(t.Values) {
		return t.Values[i], t.Keys[i], true
	}
	return nil, "", false
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

func MapToMessage(m map[string]interface{}) (*Message, error) {
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
	return &Message{Keys: columnNames, Values: values}, nil
}
