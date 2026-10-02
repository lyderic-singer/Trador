import asyncio


async def call(fn, *args, **kwargs):
    result = fn(*args, **kwargs)
    if asyncio.iscoroutine(result):
        return await result
    return result


async def cancel(task: asyncio.Task | None):
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def closests(l1: list[float], l2: list[float]):
    if not l1:
        return l2
    elif len(l2) <= len(l1):
        return []
    else:
        l = []
        j = 0
        for x in l1:
            while (j < len(l2)) and (l2[j] < x):
                l.append(l2[j])
                j += 1
            if l and ((j >= len(l2) - 1) or (abs(x - l2[j]) < abs(x - l2[j + 1]))):
                l.pop()
            else:
                j += 1
        return l + l2[j:]


def remaining(l1: list[float], l2: list[float]) -> list[float]:
    res = []
    j = 0
    for x in l1:
        while j < len(l2) and l2[j] < x:
            res.append(l2[j])
            j += 1
        if j < len(l2) and (not res or abs(l2[j] - x) <= abs(x - res[-1])):
            j += 1
        elif res:
            res.pop()
    return res + l2[j:]
