from importlib.resources import files

def load_prompt(name: str) -> str:
    """
    Read a Markdown prompt from texera_bot/prompts/{name}.md
    Usage: planner_prompt = load_prompt("planner_sys")
    """
    return (files(__package__) / "prompts" / f"{name}.md").read_text(encoding="utf-8")