use chrono::prelude::*;
use csv::Writer;
use log::warn;
use msr::*;
use std::{collections::HashMap, fs::OpenOptions, io::Result, path::PathBuf, time::Duration};

/// A simple CSV recoder implementation.
pub struct CsvRecorder {
    created_header: bool,
    states: Vec<(DateTime<Utc>, HashMap<String, Value>)>,
    cfg: CsvRecorderConfig,
}

/// CSV recorder configuration.
pub struct CsvRecorderConfig {
    /// Path to the CSV file
    pub file_name: PathBuf,
    /// List of IDs that shall be recorded.
    pub key_list: Vec<String>,
    /// Time formatting option.
    pub time_format: Option<String>,
}

/// Get a list of values names that can be recorded.
pub trait RecKeys {
    fn rec_keys(&self) -> Vec<String>;
}

/// Get a recordable map of values.
pub trait RecVals {
    fn rec_vals(&self) -> HashMap<String, Value>;
}

impl RecKeys for Loop {
    fn rec_keys(&self) -> Vec<String> {
        let mut keys = vec![format!("setpoint.{}", self.id)];
        let inputs: Vec<_> = self.inputs.iter().map(|id| format!("in.{}", id)).collect();
        let outputs: Vec<_> = self
            .outputs
            .iter()
            .map(|id| format!("out.{}", id))
            .collect();
        use crate::ControllerConfig::*;
        let (c_type, c_keys) = match self.controller {
            Pid(_) => ("pid", vec!["target", "prev_value", "p", "i", "d"]),
            BangBang(_) => ("bb", vec!["threshold", "current"]),
        };
        let cntrl: Vec<_> = c_keys
            .iter()
            .map(|key| format!("controller.{}.{}.{}", self.id, c_type, key))
            .collect();
        keys.extend_from_slice(&inputs);
        keys.extend_from_slice(&outputs);
        keys.extend_from_slice(&cntrl);
        keys
    }
}

impl RecKeys for SyncRuntime {
    fn rec_keys(&self) -> Vec<String> {
        let mut keys = vec![];
        for l in self.loops.iter() {
            keys.extend_from_slice(&l.rec_keys());
        }
        for (id, _) in self.state_machines.iter() {
            keys.push(format!("fsm.{}", id));
        }
        keys.push("rules".into());
        keys.sort();
        keys.dedup();
        keys
    }
}

impl RecVals for IoState {
    fn rec_vals(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();
        let inputs: HashMap<String, Value> = self
            .inputs
            .iter()
            .map(|(id, v)| (format!("in.{}", id), v.clone()))
            .collect();
        let outputs: HashMap<String, Value> = self
            .outputs
            .iter()
            .map(|(id, v)| (format!("out.{}", id), v.clone()))
            .collect();
        let mem: HashMap<String, Value> = self
            .mem
            .iter()
            .map(|(id, v)| (format!("mem.{}", id), v.clone()))
            .collect();
        for (k, v) in inputs {
            map.insert(k, v);
        }
        for (k, v) in outputs {
            map.insert(k, v);
        }
        for (k, v) in mem {
            map.insert(k, v);
        }
        map
    }
}

impl RecVals for SystemState {
    fn rec_vals(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();
        for (k, v) in self.io.rec_vals() {
            map.insert(k, v);
        }
        for (k, v) in self.setpoints.iter() {
            map.insert(format!("setpoint.{}", k), v.clone());
        }

        let active_rules = self
            .rules
            .iter()
            .filter(|(_, state)| **state)
            .map(|(id, _)| id)
            .cloned()
            .collect::<Vec<_>>()
            .join(",");

        map.insert("rules".into(), Value::from(active_rules));

        for (id, state) in self.state_machines.iter() {
            map.insert(format!("fsm.{}", id), Value::from(state.clone()));
        }
        for (id, c) in self.controllers.iter() {
            use crate::ControllerState::*;
            match c {
                Pid(s) => {
                    map.insert(
                        format!("controller.{}.pid.target", id),
                        Value::from(s.target),
                    );
                    if let Some(v) = s.prev_value {
                        map.insert(format!("controller.{}.pid.prev_value", id), Value::from(v));
                    }
                    map.insert(format!("controller.{}.pid.p", id), Value::from(s.p));
                    map.insert(format!("controller.{}.pid.i", id), Value::from(s.i));
                    map.insert(format!("controller.{}.pid.d", id), Value::from(s.d));
                }
                BangBang(s) => {
                    map.insert(
                        format!("controller.{}.bb.threshold", id),
                        Value::from(s.threshold),
                    );
                    map.insert(
                        format!("controller.{}.bb.current", id),
                        Value::from(s.current),
                    );
                }
            }
        }
        map
    }
}

impl CsvRecorder {
    /// Create a new recorder instance.
    pub fn new(cfg: CsvRecorderConfig) -> Self {
        CsvRecorder {
            created_header: false,
            cfg,
            states: vec![],
        }
    }

    /// Add a map of values to the internal buffer.
    pub fn record(&mut self, time: DateTime<Utc>, values: HashMap<String, Value>) {
        self.states.push((time, values));
    }

    /// Write buffred values to disk.
    pub fn persist(&mut self) -> Result<()> {
        if self.states.is_empty() {
            warn!("no states to persist");
            return Ok(());
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&self.cfg.file_name)?;

        let mut writer = Writer::from_writer(file);

        self.states.sort_by(|(t1, _), (t2, _)| t1.cmp(&t2));

        for (time, state) in self.states.iter() {
            if !self.created_header {
                let mut rec = vec!["timestamp_utc".to_string()];
                rec.extend_from_slice(self.cfg.key_list.as_slice());
                writer.write_record(rec)?;
                self.created_header = true;
            }

            let vals: Vec<_> = self
                .cfg
                .key_list
                .iter()
                .map(|key| match state.get(key) {
                    Some(v) => {
                        use crate::Value::*;
                        match v {
                            Decimal(d) => d.to_string(),
                            Integer(i) => i.to_string(),
                            Bit(b) => b.to_string(),
                            Text(t) => t.clone(),
                            Timeout(t) => (*t == Duration::new(0, 0)).to_string(),
                            Bin(_) => {
                                warn!("The binary data of '{}' will not be recorded", key);
                                "".to_string()
                            }
                        }
                    }
                    None => "".to_string(),
                })
                .collect();

            let mut rec = vec![];

            if let Some(ref fmt) = self.cfg.time_format {
                rec.push(time.format(&fmt).to_string());
            } else {
                rec.push(time.timestamp_millis().to_string());
            }
            rec.extend_from_slice(vals.as_slice());
            writer.write_record(rec)?;
        }
        writer.flush()?;
        self.states = vec![];
        Ok(())
    }
}
