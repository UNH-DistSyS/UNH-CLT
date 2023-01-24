package data_structures

type Set map[interface{}]struct{}

func NewSet() Set {
	return make(map[interface{}]struct{})
}

func NewSetFromList(l []interface{}) Set {
	s := make(map[interface{}]struct{})
	for listItem := range l {
		s[listItem] = struct{}{}
	}
	return s
}

func (s Set) Add(e interface{}) {
	s[e] = struct{}{}
}

func (s Set) Has(e interface{}) bool {
	_, exists := s[e]
	return exists
}

func (s Set) Remove(e interface{}) {
	delete(s, e)
}

func (s Set) Slice() []interface{} {
	slice := make([]interface{}, 0)
	for e := range s {
		slice = append(slice, e)
	}
	return slice
}

func (s Set) Clear() {
	for k := range s {
		delete(s, k)
	}
}

func (s Set) Clone() Set {
	clone := NewSet()
	for v := range s {
		clone.Add(v)
	}
	return clone
}

func (s Set) Union(s2 Set) Set {
	s1 := s.Clone()
	for itemInS2 := range s2 {
		s1.Add(itemInS2)
	}
	return s1
}

func (s Set) Diff(s2 Set) Set {
	s1 := s.Clone()
	for itemInS2 := range s2 {
		if s1.Has(itemInS2) {
			s1.Remove(itemInS2)
		}
	}
	return s1
}

func (s Set) ToList() []interface{} {
	l := make([]interface{}, 0, len(s))
	for item := range s {
		l = append(l, item)
	}
	return l
}
