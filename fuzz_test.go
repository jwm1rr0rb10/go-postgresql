package psql

import "testing"

func FuzzSQLVerb(f *testing.F) {
	for _, s := range []string{"SELECT 1", "-- c\nupdate t", "/* x */ (select 1)", "/*", "--", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		verb := sqlVerb(sql)
		if _, ok := knownVerbs[verb]; !ok && verb != "other" {
			t.Fatalf("sqlVerb(%q) = %q: unbounded label value", sql, verb)
		}
	})
}

func FuzzPrettySQL(f *testing.F) {
	for _, s := range []string{"SELECT  *\n\tFROM t", "SELECT 'a  b'", `"x  y"`, "'unterminated  ", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, q string) {
		once := PrettySQL(q)
		if twice := PrettySQL(once); twice != once {
			t.Fatalf("PrettySQL is not idempotent: %q -> %q -> %q", q, once, twice)
		}
	})
}
