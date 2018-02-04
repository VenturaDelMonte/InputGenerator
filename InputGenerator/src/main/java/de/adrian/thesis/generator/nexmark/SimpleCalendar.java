package de.adrian.thesis.generator.nexmark;/*
   NEXMark Generator -- Niagara Extension to XMark Data Generator

   Acknowledgements:
   The NEXMark Generator was developed using the xmlgen generator 
   from the XMark Benchmark project as a basis. The NEXMark
   generator generates streams of auction elements (bids, items
   for auctions, persons) as opposed to the auction files
   generated by xmlgen.  xmlgen was developed by Florian Waas.
   See http://www.xml-benchmark.org for information.

   Copyright (c) Dept. of  Computer Science & Engineering,
   OGI School of Science & Engineering, OHSU. All Rights Reserved.

   Permission to use, copy, modify, and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies
   of the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   THE AUTHORS AND THE DEPT. OF COMPUTER SCIENCE & ENGINEERING 
   AT OHSU ALLOW USE OF THIS SOFTWARE IN ITS "AS IS" CONDITION, 
   AND THEY DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES 
   WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

   This software was developed with support from NSF ITR award
   IIS0086002 and from DARPA through NAVY/SPAWAR 
   Contract No. N66001-99-1-8098.

*/

import java.util.Random;

class SimpleCalendar {

    private static final int MAX_INCREMENT_SEC = 60;

    private int time_in_seconds = 0; // time in seconds
    private Random rnd;

    SimpleCalendar(Random rnd) {
        this.rnd = rnd;
    }

    int getTimeInSecs() {
        return time_in_seconds;
    }

    int getTimeInMS() {
        return time_in_seconds * 1000;
    }

    synchronized void incrementTime() {
        time_in_seconds += rnd.nextInt(MAX_INCREMENT_SEC); // 1000 millesecons per second
        assert time_in_seconds >= 0 : "Time overflow";
    }
}