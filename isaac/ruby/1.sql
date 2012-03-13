SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";

--
-- Table structure for table `Connected`
--

DROP TABLE IF EXISTS `Connected`;
CREATE TABLE `Connected` (
  `FromEntityID` int(11) NOT NULL,
  `ToEntityID` int(11) NOT NULL,
  `Strength` float NOT NULL DEFAULT '1',
  `Initial` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`FromEntityID`,`ToEntityID`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Entity`
--

DROP TABLE IF EXISTS `Entity`;
CREATE TABLE `Entity` (
  `EntityID` int(11) NOT NULL AUTO_INCREMENT,
  `EntityName` varchar(150) NOT NULL,
  PRIMARY KEY (`EntityID`),
  UNIQUE KEY `EntityName` (`EntityName`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `MessageProperties`
--

DROP TABLE IF EXISTS `MessageProperties`;
CREATE TABLE `MessageProperties` (
  `MessageID` int(11) NOT NULL AUTO_INCREMENT,
  `Data` text NOT NULL,
  `TimeStamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `Restricted` tinyint(1) NOT NULL DEFAULT '1',
  `IDonNetwork` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`MessageID`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `MessageReceive`
--

DROP TABLE IF EXISTS `MessageReceive`;
CREATE TABLE `MessageReceive` (
  `MessageID` int(11) NOT NULL,
  `EntityID` int(11) NOT NULL,
  PRIMARY KEY (`MessageID`,`EntityID`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `MessageSend`
--

DROP TABLE IF EXISTS `MessageSend`;
CREATE TABLE `MessageSend` (
  `MessageID` int(11) NOT NULL,
  `EntityID` int(11) NOT NULL,
  PRIMARY KEY (`MessageID`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------