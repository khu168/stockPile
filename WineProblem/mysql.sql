CREATE DATABASE  IF NOT EXISTS `WINETASTE`;

use WINETASTE;
CREATE TABLE WishList(pid INT	not null,wid INT not null);

CREATE TABLE PersonsRank(pid	INT not null primary key, weight FLOAT);

CREATE TABLE WinesRank(wid INT NOT NULL PRIMARY KEY ,weight FLOAT NOT NULL);

CREATE INDEX WishListWidx ON WishList(wid);
CREATE INDEX WishListPidx ON WishList(pid);