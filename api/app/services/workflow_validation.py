from collections import deque

from app.schemas import WorkflowStepCreate


def validate_workflow_steps(steps: list[WorkflowStepCreate]) -> None:
    names = [step.name for step in steps]
    if len(names) != len(set(names)):
        raise ValueError("step names must be unique")

    known_names = set(names)
    dependents: dict[str, list[str]] = {name: [] for name in names}
    indegree = {name: 0 for name in names}

    for step in steps:
        if len(step.depends_on) != len(set(step.depends_on)):
            raise ValueError(f"step '{step.name}' contains duplicate dependencies")
        for dependency in step.depends_on:
            if dependency not in known_names:
                raise ValueError(
                    f"step '{step.name}' depends on unknown step '{dependency}'"
                )
            dependents[dependency].append(step.name)
            indegree[step.name] += 1

    ready = deque(name for name in names if indegree[name] == 0)
    visited = 0
    while ready:
        name = ready.popleft()
        visited += 1
        for dependent in dependents[name]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)

    if visited != len(names):
        raise ValueError("workflow dependencies contain a cycle")
