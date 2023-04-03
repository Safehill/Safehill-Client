//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/22/21.
//

import Foundation

public enum KBHexastore : String {
    case SPO = "spo"
    case SOP = "sop"
    case OPS = "ops"
    case OSP = "osp"
    case PSO = "pso"
    case POS = "pos"

    static let JOINER: String = "::"
    public static let allValues = [SPO, SOP, OPS, OSP, PSO, POS]

    public func hexaValue(subject s: Label, predicate p: Label, object o: Label) -> String {
        switch self {
        case .SPO:
            return KBHexastore.JOINER.combine(self.rawValue, s, p, o)
        case .SOP:
            return KBHexastore.JOINER.combine(self.rawValue, s, o, p)
        case .OPS:
            return KBHexastore.JOINER.combine(self.rawValue, o, p, s)
        case .OSP:
            return KBHexastore.JOINER.combine(self.rawValue, o, s, p)
        case .PSO:
            return KBHexastore.JOINER.combine(self.rawValue, p, s, o)
        case .POS:
            return KBHexastore.JOINER.combine(self.rawValue, p, o, s)
        }
    }

    func tripleValue(subject s: Label, predicate p: Label, object o: Label, weight w: Int) -> Tuple {
        switch self {
        case .SPO:
            return (s,p,o,w)
        case .SOP:
            return (s,o,p,w)
        case .OPS:
            return (o,p,s,w)
        case .OSP:
            return (o,s,p,w)
        case .PSO:
            return (p,s,o,w)
        case .POS:
            return (p,o,s,w)
        }
    }

    static func tripleValues(subject s: Label,
                             predicate p: Label,
                             object o: Label,
                             weight w: Int) -> [Tuple] {
        var store = Array<Tuple>()
        for hexatype in KBHexastore.allValues {
            store.append(hexatype.tripleValue(subject: s,
                                              predicate: p,
                                              object: o,
                                              weight: w))
        }
        return store;
    }
}
