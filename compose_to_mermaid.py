import yaml

with open("docker-compose.yml") as f:
    data = yaml.safe_load(f)

services = data.get("services", {})

print("graph LR")

# Оголошуємо вузли
for name in services:
    print(f"  {name}([{name}])")

# Зв'язки по depends_on
for name, svc in services.items():
    deps = svc.get("depends_on", {})
    if isinstance(deps, dict):
        deps = deps.keys()
    for dep in deps:
        if dep in services:
            print(f"  {name} --> {dep}")
