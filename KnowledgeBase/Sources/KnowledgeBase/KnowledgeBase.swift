//
//  KnowledgeBase.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/16/21.
//

import os

internal let log = Logger(subsystem: "com.gf.knowledgebase", category: "KnowledgeBase")

let KnowledgeBaseBundleIdentifier = "com.gf.framework.knowledgebase"

public typealias KBActionCompletion = (Swift.Result<Void, Error>) -> ()
public typealias KBObjCActionCompletion = (Error?) -> ()
