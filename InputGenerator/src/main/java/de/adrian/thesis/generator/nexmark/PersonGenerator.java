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


import de.adrian.thesis.generator.nexmark.data.*;

import java.nio.CharBuffer;
import java.util.Random;
import java.util.Vector;

public class PersonGenerator {

    public static int NUM_CATEGORIES = 1000;

    private final Random random = new Random(20934);

    class Profile {
        public Vector<String> interests = new Vector<>();

        public boolean hasEducation;
        public boolean hasGender;
        public boolean hasAge;

        public String education;
        public String gender;
        public String business;
        public String age;

        public CharBuffer income = CharBuffer.allocate(30);
    }

    class Address {
        public CharBuffer street = CharBuffer.allocate(100);
        public String city;
        public String province;
        public String country;
        public String zipcode;
    }

    public boolean hasPhone;
    public boolean hasAddress;
    public boolean hasHomepage;
    public boolean hasCreditcard;
    public boolean hasProfile;
    public boolean hasWatches;

    public CharBuffer name = CharBuffer.allocate(100);
    public CharBuffer email = CharBuffer.allocate(100);
    public CharBuffer phone = CharBuffer.allocate(15);
    public Address address = new Address();
    public CharBuffer homepage = CharBuffer.allocate(100);
    public CharBuffer creditcard = CharBuffer.allocate(20);
    public Profile profile = new Profile();
    public Vector watches = new Vector();

    public void generateValues(OpenAuctions auctions) {
        int ifn = random.nextInt(Firstnames.NUM_FIRSTNAMES);
        int iln = random.nextInt(Lastnames.NUM_LASTNAMES);
        int iem = random.nextInt(Emails.NUM_EMAILS);

        name.clear();
        name.put(Firstnames.FIRSTNAMES[ifn]);
        name.put(" ");
        name.put(Lastnames.LASTNAMES[iln]);

        email.clear();
        email.put(Lastnames.LASTNAMES[iln]);
        email.put("@");
        email.put(Emails.EMAILS[iem]);

        if (random.nextBoolean()) {
            hasPhone = true;
            phone.clear();
            phone.put("+");
            phone.put(NumberMapping.STRINGS[random.nextInt(98) + 1]);
            phone.put("(");
            phone.put(NumberMapping.STRINGS[random.nextInt(989) + 10]);
            phone.put(")");
            phone.put(String.valueOf(random.nextInt(9864196) + 123457));
        } else {
            hasPhone = false;
        }

        if (random.nextBoolean()) {
            hasAddress = true;
            genAddress();
        } else {
            hasAddress = false;
        }

        if (random.nextBoolean()) {
            hasHomepage = true;
            homepage.clear();
            homepage.put("http://www.");
            homepage.put(Emails.EMAILS[iem]);
            homepage.put("/~");
            homepage.put(Lastnames.LASTNAMES[iln]);
        } else {
            hasHomepage = false;
        }

        if (random.nextBoolean()) {
            hasCreditcard = true;
            creditcard.clear();
            creditcard.put(String.valueOf(random.nextInt(9000) + 1000)); //HERE
            creditcard.put(" ");
            creditcard.put(String.valueOf(random.nextInt(9000) + 1000)); //HERE
            creditcard.put(" ");
            creditcard.put(String.valueOf(random.nextInt(9000) + 1000)); //HERE
            creditcard.put(" ");
            creditcard.put(String.valueOf(random.nextInt(9000) + 1000)); //HERE
        } else {
            hasCreditcard = false;
        }

        if (random.nextBoolean()) {
            hasProfile = true;
            genProfile();
        } else {
            hasProfile = false;
        }

        hasWatches = false;
        /* skip watches for now -  expensive and problem with
         * people who are generated before any items
         if (random.nextBoolean()) {
         int cWatches = random.nextInt(20) + 1;
         int iWatch;
         for (int i=0; i<cWatches; i++) {
         // is this OK, will this screw up bids/items distribution??
         watches.add(String.valueOf(auctions.getExistingId()));
         }
         } else {
         watches.clear();
         }*/
    }

    private void genAddress() {
        int ist = random.nextInt(Lastnames.NUM_LASTNAMES); // street
        int ict = random.nextInt(Cities.NUM_CITIES); // city
        int icn = (random.nextInt(4) != 0) ? 0 :
                random.nextInt(Countries.NUM_COUNTRIES);
        int ipv = (icn == 0) ? random.nextInt(Provinces.NUM_PROVINCES) :
                random.nextInt(Lastnames.NUM_LASTNAMES);  // PROVINCES are really states

        address.street.clear();
        address.street.put(String.valueOf((random.nextInt(99) + 1)));
        address.street.put(" ");
        address.street.put(Lastnames.LASTNAMES[ist]);
        address.street.put(" St");

        address.city = Cities.CITIES[ict];

        if (icn == 0) {
            address.country = "United States";
            address.province = Provinces.PROVINCES[ipv];
        } else {
            address.country = Countries.COUNTRIES[icn];
            address.province = Lastnames.LASTNAMES[ipv];
        }
        address.zipcode = String.valueOf(random.nextInt(99999) + 1);
    }

    private void genProfile() {
        if (random.nextBoolean()) {
            profile.hasEducation = true;
            profile.education =
                    Education.EDUCATION[random.nextInt(Education.NUM_EDUCATION)];
        } else {
            profile.hasEducation = false;
        }

        if (random.nextBoolean()) {
            profile.hasGender = true;
            profile.gender = (random.nextInt(2) == 1) ? "male" : "female";
        } else {
            profile.hasGender = false;
        }

        profile.business = (random.nextInt(2) == 1) ? "Yes" : "No";

        if (random.nextBoolean()) {
            profile.hasAge = true;
            profile.age = NumberMapping.STRINGS[random.nextInt(15) + 30]; // HERE
        } else {
            profile.hasAge = false;
        }

        // incomes are zipfian - change this if we start to use
        // income values KT
        profile.income.clear();
        profile.income.put(String.valueOf((random.nextInt(30000) + 40000)));
        profile.income.put(".");
        profile.income.put(NumberMapping.STRINGS[random.nextInt(99)]); //  HERE

        int interests = random.nextInt(5);
        profile.interests.setSize(0);
        for (int i = 0; i < interests; i++) {
            profile.interests.add(String.valueOf(random.nextInt(NUM_CATEGORIES)));
        }
    }
}