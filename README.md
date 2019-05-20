## Hailstorm
A Distributed Globally Unique Id generator inspired by Twitter Snowflake. 

### Usage

```
// on each server, increment this id
Hailstorm hailstorm = new HailStorm(1 ... 1024);
long globallyUniqueId = hailStorm.generate();
```
