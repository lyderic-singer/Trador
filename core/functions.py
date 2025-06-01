import re

filename = "info.py"

with open(filename, "r", encoding="utf-8") as f:
    content = f.read()

func_names = re.findall(r"def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", content)

for name in func_names:
    print(name)

print(f"\nTotal fonctions : {len(func_names)}")
