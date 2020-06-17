package autopilot

import (
	"testing"

	"github.com/btcsuite/btcutil"
	"github.com/stretchr/testify/require"
)

// TestTopCentralityEmptyGraph tests that we don't accidentally return a score
// for an empty graph.
func TestTopCentralityEmptyGraph(t *testing.T) {
	topCentrality := NewTopCentrality()

	pub, err := randKey()
	require.NoError(t, err, "unable to generate key")

	nodes := map[NodeID]struct{}{
		NewNodeID(pub): {},
	}

	for _, chanGraph := range chanGraphs {
		graph, cleanup, err := chanGraph.genFunc()
		success := t.Run(chanGraph.name, func(t *testing.T) {
			require.NoError(t, err, "unable to create graph")
			if cleanup != nil {
				defer cleanup()
			}

			const chanSize = btcutil.SatoshiPerBitcoin

			// Attempt to get centrality scores for the empty
			// graph, and expect no scores returned.
			scores, err := topCentrality.NodeScores(
				graph, nil, chanSize, nodes,
			)

			require.NoError(t, err)
			require.Equal(t, 0, len(scores))
		})

		require.True(t, success)
	}
}

// TestTopCentralityNonEmptyGraph tests that we return the correct normalized
// centralitiy values given a non empty graph, correctly filtered down to the
// passed nodes and omiting nodes which we have channels with.
func TestTopCentralityNonEmptyGraph(t *testing.T) {
	topCentrality := NewTopCentrality()

	for _, chanGraph := range chanGraphs {
		graph, cleanup, err := chanGraph.genFunc()
		success := t.Run(chanGraph.name, func(t *testing.T) {
			require.NoError(t, err, "unable to create graph")
			if cleanup != nil {
				defer cleanup()
			}

			// Build the test graph.
			graphNodes := buildTestGraph(t, graph, centralityTestGraph)

			// Have a channel with the first graph node.
			chans := []Channel{
				{
					Node: NewNodeID(graphNodes[0]),
				},
			}

			for i := 0; i < len(graphNodes); i++ {
				nodes := make(map[NodeID]struct{})
				expected := make(map[NodeID]*NodeScore)

				for j := 0; j < i; j++ {
					// Add node to the interest set.
					nodeID := NewNodeID(graphNodes[j])
					nodes[nodeID] = struct{}{}

					// Add to the expected set unless it's the node
					// we have a channel with.
					if j != 0 {
						expected[nodeID] = &NodeScore{
							NodeID: nodeID,
							Score:  normalizedTestGraphCentrality[j],
						}
					}
				}

				const chanSize = btcutil.SatoshiPerBitcoin

				// Attempt to get centrality scores and expect
				// that the result equals with the expected set.
				scores, err := topCentrality.NodeScores(
					graph, chans, chanSize, nodes,
				)

				require.NoError(t, err)
				require.Equal(t, expected, scores)
			}
		})

		require.True(t, success)
	}
}
