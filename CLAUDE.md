# CLAUDE.md

## Grenton Lua (skrypty w `Local Files/Grenton Source Code/`)

- Prawdziwość wartości działa jak w standardowym Lua: liczby (także 0) są prawdziwe, fałszywe są tylko `nil` i `false`. Sprawdzone na CLU: `if 5 then ... end` wykonuje się.
- **Nie używaj `tonumber(...)` na odczytach obiektów Grenton (`MOD01->m...->Value`, `v...`).** Ten kod się nie wykonał:
  ```lua
  local pvPower = tonumber(MOD01->mSUN2000_InputPower->Value)
  if pvPower and pvPower >= 0 and pvPower <= 12000 then  -- warunek dał false
  ```
  Prawdopodobna przyczyna: `tonumber()` zwraca `nil` dla `->Value`. Nie jest to jeszcze potwierdzone, można to sprawdzić przez `type(tonumber(...->Value))`.
- Odczyty porównuj bezpośrednio (`if x >= 0 and x <= 12000 then`), tak jak w istniejących gałęziach `sPVControl.lua`.
