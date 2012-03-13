#!/usr/bin/ruby

require "ruby-graphml"

def graphml_to_tgf(path)
  parser = GraphML::Parser.new
  Nokogiri::XML::SAX::Parser.new(parser).parse(File.open(path))
  nodes = parser.graph.nodes.map {|node| node[0]}
  edges = parser.graph.edges.map do |edge| 
    n1,n2 = edge.source.id, edge.target.id
    [nodes.find_index(n1)+1,nodes.find_index(n2)+1]
  end
  nodes.each_with_index {|n,i| puts "#{i+1} #{n}"}
  edges.each {|n1,n2| puts "#{n1} #{n2}"}
end

graphml_to_tgf ARGV[0]
