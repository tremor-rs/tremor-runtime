This code doesn't work since having `&self.contraflow` in the for loop borroes 
```rust
        for idx in &self.contraflow {
          let res = {
            let op = self.graph.get_mut(*idx).unwrap(); // We know this exists
            op.on_signal(&mut signal)?
          };
          self.enqueue_events(*idx, res);
          returns.append(&mut self.run()?)
        }
```

Translating it to this works:
```rust
        for idx in 0..self.contraflow.len() {
            let i = self.contraflow[idx];
            let res = {
                let op = self.graph.get_mut(i).unwrap(); // We know this exists
                op.on_signal(&mut signal)?
            };
            self.enqueue_events(i, res);
            returns.append(&mut self.run()?)
        }
```
