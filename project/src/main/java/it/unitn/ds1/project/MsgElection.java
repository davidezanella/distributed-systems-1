package it.unitn.ds1.project;

import java.io.Serializable;
import java.util.HashMap;

public class MsgElection implements Serializable {
    HashMap<Integer, MsgWriteOK> nodesHistory = new HashMap<>();
    HashMap<Integer, Boolean> seen = new HashMap<>();

    @Override
    public String toString() {
        return "election";
    }
}
