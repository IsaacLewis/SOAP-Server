#!/usr/bin/ruby

def read_file(filename, &block)
  file = File.open(filename, "r")
  file.each &block
end

def write_file(filename, data)
  file = File.open(filename, "w")
  data.each do |d|
    file.write d+"\n"
  end
end

def go!
  $inserts = []
  read_file "indycascade.txt", do |line|
    $inserts << parse_line(line)
  end

  write_file "out.txt", $inserts
end

def parse_line(line)
  words = line.split
  case words.length
  when 4
    # its a node
    insert_entity words[0], words[1]
  when 6
    # its an edge
    insert_edge words[0], words[1], words[2]
  else
    # fail
    puts "didn't parse line:#{line}, invalid"
  end
end

def insert_entity(id, name)
  "INSERT INTO `Entity` (`EntityID`,`EntityName`) VALUES (#{id},'#{name}');"
end

def insert_edge(from_id, to_id, strength)
  "INSERT INTO `Connected` (`FromEntityID`, `ToEntityID`, `Strength`, `Initial`) (#{from_id}, #{to_id}, strength, 1);"
end

def make_tables
  <<SQL
CREATE TABLE IF NOT EXISTS `Connected` (
  `FromEntityID` int(11) NOT NULL,
  `ToEntityID` int(11) NOT NULL,
  `Strength` float NOT NULL DEFAULT '1',
  `Initial` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`FromEntityID`,`ToEntityID`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `Entity` (
  `EntityID` int(11) NOT NULL AUTO_INCREMENT,
  `EntityName` varchar(150) NOT NULL,
  PRIMARY KEY (`EntityID`),
  UNIQUE KEY `EntityName` (`EntityName`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `MessageProperties` (
  `MessageID` int(11) NOT NULL AUTO_INCREMENT,
  `Data` text NOT NULL,
  `TimeStamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `Restricted` tinyint(1) NOT NULL DEFAULT '1',
  `IDonNetwork` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`MessageID`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `MessageReceive` (
  `MessageID` int(11) NOT NULL,
  `EntityID` int(11) NOT NULL,
  PRIMARY KEY (`MessageID`,`EntityID`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `MessageSend` (
  `MessageID` int(11) NOT NULL,
  `EntityID` int(11) NOT NULL,
  PRIMARY KEY (`MessageID`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

SQL
end
