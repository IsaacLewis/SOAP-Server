#!/usr/bin/ruby

class Node
  attr_reader :label
  attr_reader :node_id
  attr_accessor :influenced

  @@nodes = []
  @@edges = []

  def self.nodes; @@nodes; end
  def self.edges; @@edges; end
  def self.reset; @@nodes = []; @@edges = []; end

  def self.add_edge()
    while true
      n1,n2 = @@nodes.sample.node_id, @@nodes.sample.node_id

      next if n1 == n2
      edge = if n1 < n2
               [n1,n2]
             else
               [n2,n1]
             end

      weight = rand * 0.2
      edge.push weight

      if @@edges.any? {|n1,n2,weight| n1 == edge[0] && n2 == edge[1]}
        next
      else
        @@edges.push(edge)
        break
      end
    end
  end

  def initialize
    @label = get_label
    @node_id = get_id
    @@nodes.push(self)
    @influenced = false
  end
end


def generate(nodes, links_per_node, num_influenced, filename)
  file = File.new filename, "w"
  Node.reset
  $label = nil
  $id = 0

  nodes.times do |i|
    puts i
    Node.new
  end

  (nodes * links_per_node / 2).times do |i|
    puts i
    Node.add_edge
  end

  num_influenced.times {Node.nodes.sample.influenced = true}

  Node.nodes.each {|n| file.write "#{n.node_id} #{n.label} #{n.influenced}\n"}
  Node.edges.each {|n1,n2,weight| file.write "#{n1} #{n2} #{weight}\n"}
  file.close
end

def get_label
  $label = ($label ? $label.next : "A")
  return $label
end

def get_id
  $id += 1
  return $id
end

generate ARGV[0].to_i, ARGV[1].to_i, ARGV[2].to_i, ARGV[3]
