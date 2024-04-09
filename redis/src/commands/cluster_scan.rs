use crate::cluster::ClusterConnection;
use crate::cluster_routing::{Route, SlotAddr};
const BITS_PER_U64: usize = 64;
const NUM_OF_SLOTS: usize = 16384;
const BITS_ARRAY_SIZE: usize = NUM_OF_SLOTS / BITS_PER_U64;
type SlotsBitsArray = [u64; BITS_ARRAY_SIZE];
struct ScanState {
    pub cursor: usize,
    pub scanned_slots_map: SlotsBitsArray,
    pub slots_in_scan_map: SlotsBitsArray,
    pub node_in_scan: String,
    pub node_epoch: u64,
}

impl ScanState {
    pub fn new() -> Self {
        Self {
            cursor: 0,
            scanned_slots_map: [0; BITS_ARRAY_SIZE],
            slots_in_scan_map: [0; BITS_ARRAY_SIZE],
            node_in_scan: "".to_string(),
            node_epoch: 0,
        }
    }
    pub fn get_next_slot(&self) -> Option<usize> {
        for (word_index, &word) in self.scanned_slots_map.iter().enumerate() {
            if word != u64::MAX {
                for bit_offset in 0..BITS_PER_U64 {
                    if (word & (1 << bit_offset)) == 0 {
                        return Some(word_index * BITS_PER_U64 + bit_offset);
                    }
                }
            }
        }
        None
    }
    pub fn set_slot(&mut self, slot: usize) {
        let word_index = slot / BITS_PER_U64;
        let bit_offset = slot % BITS_PER_U64;
        self.scanned_slots_map[word_index] |= 1 << bit_offset;
    }
    pub fn set_slots_in_scan_as_scanned(&mut self) {
        for i in 0..BITS_ARRAY_SIZE {
            self.scanned_slots_map[i] |= self.slots_in_scan_map[i];
        }
    }
    pub fn get_next_node_to_scan(&self, cluster: ClusterConnection) -> String {
        let next_slot_to_scan = self.get_next_slot().unwrap();
        let binding = cluster.get_slot_map();
        let slot_map = binding.borrow();
        let node_adrrs = slot_map.slot_addr_for_route(&Route::new(
            next_slot_to_scan as u16,
            SlotAddr::ReplicaOptional,
        ));
        node_adrrs.unwrap().to_string()
    }
    pub fn get_slots_of_node(&self, node_addres: &String, cluster: ClusterConnection) -> Vec<u16> {
        let mut slots = Vec::new();
        let binding = cluster.get_slot_map();
        let cluster_slotmap = binding.borrow();
        for (slot, slot_map_value) in cluster_slotmap.get_slots_map_values().iter() {
            if &slot_map_value.addrs.primary == node_addres
                || slot_map_value.addrs.replicas.contains(node_addres)
            {
                slots.push(slot.to_owned());
            }
        }
        slots
    }
}
