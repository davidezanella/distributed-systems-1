package it.unitn.ds1.project;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class MsgCoordinator implements Serializable {
    HashMap<Integer, ArrayList<MsgWriteOK>> nodesHistory = new HashMap<>();
}
