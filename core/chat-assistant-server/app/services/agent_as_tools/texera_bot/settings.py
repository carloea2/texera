from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    planner_model: str = "gpt-4.1"
    builder_model: str = "gpt-4o-mini"
    manager_model: str = "gpt-4o-mini"
    temperature: float = 0.1
    top_p: float = 1.0
    viz_dir: str = "viz"
