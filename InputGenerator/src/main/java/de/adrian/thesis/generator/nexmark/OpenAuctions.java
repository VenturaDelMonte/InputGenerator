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

// class to generate IDs for the auction generator
// is capable of generating new ids or generating existing IDs
// distributions supported are exponential and uniform

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

class OpenAuctions {

    private static final ConcurrentHashMap<Long, OpenAuction> OPEN_AUCTIONS =
            new ConcurrentHashMap<>(1_000_000, 0.8f, 8);
    private static final AtomicLong AUCTION_IDS = new AtomicLong();

    private final ThreadLocalRandom random;
    private final SimpleCalendar calendar;

    OpenAuctions(SimpleCalendar calendar, ThreadLocalRandom random) {
        this.calendar = calendar;
        this.random = random;
    }

    public long createNewAuction() {
        long auctionId = AUCTION_IDS.getAndIncrement();
        OpenAuction newAuction = new OpenAuction(calendar, auctionId, random);
        OPEN_AUCTIONS.put(auctionId, newAuction);
        return auctionId;
    }

    public long getExistingId() {
        return random.nextLong(AUCTION_IDS.get());
    }

    public synchronized int increasePrice(long id) {
        return OPEN_AUCTIONS.get(id).increasePrice();
    }

    public long getEndTime(long id) {
        return OPEN_AUCTIONS.get(id).getEndTime();
    }

    public synchronized int getCurrPrice(long id) {
        return OPEN_AUCTIONS.get(id).getCurrPrice();
    }
}

