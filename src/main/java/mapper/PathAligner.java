package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeSet;

// A PathAligner aligns two sequences by exploring possible alignments mostly along a path
// A PathAligner doesn't consider the possibility of an ungapped alignment because that it supposed to be checked by a StraightAligner
public class PathAligner {
  public PathAligner(Logger logger) {
    this.logger = logger;
  }

  private boolean chooseSearchReverse() {
    int sumOfMismatchingIndices = 0;
    int numMismatches = 0;
    int sumOfMatchingIndices = 0;
    int numMatches = 0;

    Sequence sequenceA = this.query;
    Sequence sequenceB = this.reference;
    int offset = this.alignmentAnalysis.predictedBestOffset;

    int startIndex = Math.max(this.startIndexA, this.startIndexB - offset);
    int endIndex = Math.min(this.endIndexA, this.endIndexB - offset);
    int length = endIndex - startIndex;

    for (int i = 0; i < length; i++) {
      int j = i - diagonal;
      if (j >= 0 && j < this.referenceEncodedChars.length) {
        byte a = getEncodedCharA(i);
        byte b = getEncodedCharB(j);
        if (!Basepairs.canMatch(a, b)) {
          sumOfMismatchingIndices += i;
          numMismatches++;
        } else {
          sumOfMatchingIndices += i;
          numMatches++;
        }
      }

    }
    if (numMismatches > 1 && numMatches > 1) {
      int averageMismatchIndex = sumOfMismatchingIndices / numMismatches;
      int averageMatchIndex = sumOfMatchingIndices / numMatches;
      return (averageMismatchIndex > averageMatchIndex);
    } else {
      return true;
    }
  }

  public SequenceAlignment align(SequenceSection querySection, SequenceSection referenceSection, AlignmentParameters parameters, AlignmentAnalysis alignmentAnalysis) {
    // setup

    // save some parameters
    this.parameters = parameters;
    this.maxInterestingPenalty = querySection.getLength() * parameters.MaxErrorRate;

    if (logger.getEnabled()) {
      logger.log("PathAligner looking for penalty <= " + this.maxInterestingPenalty);
    }
    // set up some data structures
    this.prioritizedNodes = new HashMap<Double, List<AlignmentNode>>();
    this.priorities = new PriorityQueue();
    this.locatedNodes = new ArrayList<List<AlignmentNode>>();

    this.query = querySection.getSequence();
    this.startIndexA = querySection.getStartIndex();
    this.endIndexA = querySection.getEndIndex();
    this.queryEncodedChars = this.sequenceToEncodedChars(query, startIndexA, endIndexA);
    this.reference = referenceSection.getSequence();
    this.startIndexB = referenceSection.getStartIndex();
    this.endIndexB = referenceSection.getEndIndex();
    this.referenceEncodedChars = this.sequenceToEncodedChars(reference, startIndexB, endIndexB);
    this.textALength = querySection.getLength();
    this.textBLength = referenceSection.getLength();
    this.alignmentAnalysis = alignmentAnalysis;
    this.diagonal = this.startIndexB - (this.startIndexA + this.alignmentAnalysis.predictedBestOffset);
    this.searchReverse = this.chooseSearchReverse();
    if (logger.getEnabled()) {
      logger.log("PathAligner aligning " + querySection.format() + " to " + referenceSection.format());
      logger.log("PathAligner searchReverse = " + searchReverse + ", confidentAboutBestOffset = " + alignmentAnalysis.confidentAboutBestOffset + ", max interesting penalty = " + this.maxInterestingPenalty);
    }
    if (searchReverse) {
      this.stepDelta = -1;
      this.mayQueryExtendPastEndOfReference = this.startIndexB == 0;
    } else {
      this.stepDelta = 1;
      this.mayQueryExtendPastEndOfReference = this.endIndexB == this.reference.getLength();
    }

    // more setup
    Sequence sequenceA = querySection.getSequence();
    Sequence sequenceB = referenceSection.getSequence();
    int startIndexA = querySection.getStartIndex();
    int endIndexA = querySection.getEndIndex();

    // sequenceA for the width, sequenceB for the height
    int width = this.textALength + 2;
    int height = endIndexB - startIndexB + 2;

    // add all of the starting nodes
    // We need one row and column in front of the grid to allow us to determine where to start
    // We also leave one row and column extra at the end of the grid in case we're searching in reverse
    if (this.searchReverse) {
      startX = width - 1;
      startY = height - 1;
      goalX = 1;
      goalY = 1;
    } else {
      startX = 0;
      startY = 0;
      goalX = width - 2;
      goalY = height - 2;
    }

    if (this.textBLength >= this.textALength) {
      // check for initial deletions
      double startingInsertionStartPenalty = parameters.getStartingInsertionStartPenalty();
      if (!this.mayQueryExtendPastEndOfReference)
        startingInsertionStartPenalty = disallowed;
      // check for initial deletions
      int initialDeletionCount;
      initialDeletionCount = Math.max(0, this.textBLength - this.textALength) + 1;
      for (int i = 0; i < initialDeletionCount; i++) {
        int ya = startY + i * this.stepDelta;
        this.putNode(new AlignmentNode(startX, ya, 0, startingInsertionStartPenalty, disallowed, false, false));
      }
    } else {
      // check for initial insertions
      int initialInsertionCount = Math.max(0, this.textALength - this.textBLength) + 1;
      for (int i = 0; i < initialInsertionCount; i++) {
        int xa = startX + i * this.stepDelta;
        this.putNode(new AlignmentNode(xa, startY, 0, disallowed, disallowed, false, false));
      }
    }

    if (this.mayQueryExtendPastEndOfReference) {
      double startingInsertionStartPenalty = parameters.getStartingInsertionStartPenalty();
      // check for initial insertions
      int initialInsertionCount = (int)(alignmentAnalysis.maxInsertionExtensionPenalty / parameters.DeletionExtension_Penalty);
      for (int i = 1; i < initialInsertionCount; i++) {
        int xa = startX + i * this.stepDelta;
        double penalty = i * parameters.UnalignedPenalty;
        this.putNode(new AlignmentNode(xa, startY, penalty, disallowed, disallowed, false, false));
      }
    }


    AlignmentNode lastNode = null;
    int numSteps = 0;
    while (lastNode == null) {
      if (this.priorities.size() < 1 && this.logger.getEnabled()) {
        this.outputDiagnostics();
      }
      this.activePenalty = this.priorities.poll();
      List<AlignmentNode> nodes = this.prioritizedNodes.get(this.activePenalty);
      for (int i = 0; i < nodes.size(); i++) {
        numSteps++;
        AlignmentNode node = nodes.get(i); //nodes.size() - 1);
        //nodes.remove(nodes.size() - 1);

        int x = node.getX();
        int y = node.getY();
        // add a little bit extra penalty to the threshold for rounding error
        if (this.activePenalty > this.maxInterestingPenalty + 0.000001) {
          // We process nodes in order of how large their penalty is
          // Once we find one node whose penalty is too high, all of the remaining nodes must have penalties that are too high too
          if (logger.getEnabled()) {
            logger.log("PathAligner finds no sufficient alignment exists after " + numSteps + " steps at " + this.reference.getName() + " position " + this.startIndexB + " because node penalty = " + this.activePenalty + " at " + x + "," + y + " > " + this.maxInterestingPenalty);
          }
          maybeOutputDiagnostics();
          return null;
        }

        // If we reached the end of the query section or of the reference, we're done
        if (x == goalX) {
          maybeOutputDiagnostics();
          lastNode = node;
          if (logger.getEnabled()) {
            logger.log("PathAligner found an answer at " + x + ", " + y + " after " + numSteps + " steps");
          }
          break;
        }
        // update the penalties for adjacent nodes
        this.explore(x, y);
      }
      this.prioritizedNodes.remove(this.activePenalty);
    }
    // Now we've found the penalty of the best path
    // Now we walk backwards over the path to get the contents of the best path
    int i = lastNode.getX();
    int j = lastNode.getY();
    List<AlignedBlock> blocks = new ArrayList<AlignedBlock>();
    while (i != startX && j != startY) {
      AlignmentNode node = this.getNode(i, j);
      //this.logger.log("Penalty of node " + i + "," + j + " = " + node.getPenalty() + " estimated future penalty = " + estimateOverallPenalty(node) + ", dist from diagonal = " + getDistanceFromDiagonal(i, j));
      double bestPenalty = node.getPenalty();
      double insertXPenalty = node.getInsertXPenalty();
      double insertYPenalty = node.getInsertYPenalty();
      if (bestPenalty == insertXPenalty) {
        // Advance as far in this direction as possible
        // We make sure to make a single block for this because the penalty of a new insertion might be different from the penalty of extending an existing insertion
        int oldI = i;
        i -= this.stepDelta;
        while (i != startX) {
          AlignmentNode other = this.getNode(i, j);
          // Determine whether it is cheaper for us to have gotten here as a new insertion or an extension of an existing insertion
          double otherNewInsertionPenalty = other.getPenalty() + parameters.InsertionStart_Penalty + parameters.InsertionExtension_Penalty;
          double otherExtendInsertionPenalty = other.getInsertXPenalty() + parameters.InsertionExtension_Penalty;
          if (otherNewInsertionPenalty < otherExtendInsertionPenalty) {
            break;
          }
          i -= this.stepDelta;
        }
        if (this.searchReverse)
          blocks.add(new AlignedBlock(sequenceA, sequenceB, startIndexA + oldI - 1, startIndexB + j - 1, i - oldI, 0));
        else
          blocks.add(new AlignedBlock(sequenceA, sequenceB, startIndexA + i, startIndexB + j, oldI - i, 0));
      } else {
        if (bestPenalty == insertYPenalty) {
          // Advance as far in this direction as possible
          // We make sure to make a single block for this because the penalty of a new insertion might be different from the penalty of extending an existing insertion
          int oldJ = j;
          j -= this.stepDelta;
          while (j != startY) {
            AlignmentNode other = this.getNode(i, j);
            // Determine whether it is cheaper for us to have gotten here as a new deletion or as an extension of an existing deletion
            double otherNewDeletionPenalty = other.getPenalty() + parameters.DeletionStart_Penalty + parameters.DeletionExtension_Penalty;
            double otherExtendDeletionPenalty = other.getInsertYPenalty() + parameters.DeletionExtension_Penalty;
            if (otherNewDeletionPenalty < otherExtendDeletionPenalty) {
              break;
            }
            j -= stepDelta;
          }
          if (this.searchReverse)
            blocks.add(new AlignedBlock(sequenceA, sequenceB, startIndexA + i - 1, startIndexB + oldJ - 1, 0, j - oldJ));
          else
            blocks.add(new AlignedBlock(sequenceA, sequenceB, startIndexA + i, startIndexB + j, 0, oldJ - j));
        } else {
          // We create one AlignedBlock that covers as much distance in this direction as possible, to save memory
          int oldI = i;
          int oldJ = j;
          i -= stepDelta;
          j -= stepDelta;
          while (i != startX && j != startY) {
            AlignmentNode other = this.getNode(i, j);
            if (other.getPenalty() == other.getInsertXPenalty() || other.getPenalty() == other.getInsertYPenalty()) {
              break;
            }
            //this.logger.log("Penalty of node " + i + "," + j + " = " + node.getPenalty() + " estimated future penalty = " + estimateOverallPenalty(node) + ", dist from diagonal = " + getDistanceFromDiagonal(i, j));
            i -= stepDelta;
            j -= stepDelta;
          }
          if (this.searchReverse)
            blocks.add(new AlignedBlock(sequenceA, sequenceB, startIndexA + oldI - 1, startIndexB + oldJ - 1, i - oldI, j - oldJ));
          else
            blocks.add(new AlignedBlock(sequenceA, sequenceB, startIndexA + i, startIndexB + j, oldI - i, oldJ - j));
        }
      }
    }

    if (!this.searchReverse) {
      // If our initial search runs forward, then our final blocks are in the wrong order
      reverseList(blocks);
    }

    if (blocks.size() < 1) {
      if (logger.getEnabled()) {
        logger.log("PathAligner best alignment is empty");
      }
      return null;
    }

    boolean justifyLeft = false;
    SequenceAlignment result = justify(blocks);
    if (logger.getEnabled()) {
      logger.log("PathAligner found alignment at " + this.reference.getName() + " position " + result.getStartIndexB() + " aligned penalty = " + result.getAlignedPenalty());
      logger.log("Query:" + result.getAlignedTextA());
      logger.log("Ref  :" + result.getAlignedTextB());
    }
    // check for rounding error
    if (result.getAlignedPenalty() > this.maxInterestingPenalty) {
      if (logger.getEnabled()) {
        logger.log("PathAligner alignment penalty too high: aligned penalty " + result.getAlignedPenalty() + " > " + this.maxInterestingPenalty);
      }
      return null;
    }
    return result;
  }


  private void swapQueryAndReference(List<AlignedBlock> sections) {
    for (int i = 0; i < sections.size(); i++) {
      AlignedBlock existing = sections.get(i);
      AlignedBlock newBlock = new AlignedBlock(existing.getSequenceB(), existing.getSequenceA(), existing.getStartIndexB(), existing.getStartIndexA(), existing.getLengthB(), existing.getLengthA());
      sections.set(i, newBlock);
    }
  }

  // Sometimes there are multiple Alignments that have equal penalty.
  // In particular, sometimes an insertion or deletion can be moved and keep the same penalty.
  // This function attempts to shift the insertions in an effort to break ties consistently when reads are reversed
  private SequenceAlignment justify(List<AlignedBlock> sections) {
    // goLeft = false
    for (int i = 1; i < sections.size() - 1; i++) {
      while (true) {
        AlignedBlock left = sections.get(i - 1);
        AlignedBlock middle = sections.get(i);
        AlignedBlock right = sections.get(i + 1);
        if ((middle.getLengthA() > 0) == (middle.getLengthB() > 0)) {
          break; // not an indel
        }
        if (left.getLengthA() == 0 || left.getLengthB() == 0) {
          break; // nothing left to take
        }
        if (right.getLengthA() == 0 || right.getLengthB() == 0) {
          // cannot add matching basepairs into an indel
          break;
        }
        if (middle.getLengthA() > 0) {
          // lengthA is larger; shift characters right across an insertion
          if (left.getLastEncodedCharA() != middle.getLastEncodedCharA()) {
            // character does not match; cannot shift
            break;
          }
        } else {
          // lengthB is larger; shift characters right across a deletion
          if (left.getLastEncodedCharB() != middle.getLastEncodedCharB()) {
            // character does not match; cannot shift
            break;
          }
        }
        left = new AlignedBlock(left.getSequenceA(), left.getSequenceB(), left.getStartIndexA(), left.getStartIndexB(), left.getLengthA() - 1, left.getLengthB() - 1);
        middle = new AlignedBlock(middle.getSequenceA(), middle.getSequenceB(), middle.getStartIndexA() - 1, middle.getStartIndexB() - 1, middle.getLengthA(), middle.getLengthB());
        right = new AlignedBlock(right.getSequenceA(), right.getSequenceB(), right.getStartIndexA() - 1, right.getStartIndexB() - 1, right.getLengthA() + 1, right.getLengthB() + 1);
        sections.set(i - 1, left);
        sections.set(i, middle);
        sections.set(i + 1, right);
        if (this.logger.getEnabled()) {
          this.logger.log("PathAligner.justify shifted right at " + left.getStartIndexA());
        }
      }
    }
    // If any initial sections became empty, remove them too
    while (this.canRemoveSection(sections.get(0)))
      sections.remove(0);
    return new SequenceAlignment(sections, parameters, (this.query.getComplementedFrom() != null));
  }

  private void maybeOutputDiagnostics() {
    //outputDiagnostics();
  }

  private boolean canRemoveSection(AlignedBlock block) {
    if (block.getLengthA() <= 0 && block.getLengthB() <= 0) {
      return true;
    }
    if ((block.getStartIndexA() <= 0 && block.getLengthA() <= 0) || (block.getStartIndexB() <= 0 && block.getLengthB() <= 0)) {
      return true;
    }
    return false;
  }

  private void outputDiagnostics() {
    this.outputGrid();
    for (double key : this.prioritizedNodes.keySet()) {
      if (key < this.activePenalty) {
        List<AlignmentNode> nodes = this.prioritizedNodes.get(key);
        if (nodes.size() > 0) {
          this.logger.log("Invalid node list priority = " + key + " length = " + nodes.size());
        }
      }
    }
  }

  private void outputGrid() {
    String header = " ";
    for (int x = 0; x < this.textALength; x++) {
      header += "  " + Basepairs.decode(this.getEncodedCharA(x));
    }
    this.logger.log(header);
    for (int y = 0; y <= this.textBLength + 1; y++) {
      boolean empty = true;
      String text = "";
      for (int i = 0; i < 4; i++) {
        if (i == 0) {
          if (y > 0 && y <= this.textBLength)
            text = Basepairs.decode(this.getEncodedCharB(y - 1)) + " ";
          else
            text = "  ";
        } else {
          text = "  ";
        }
        String bestText = text;
        for (int x = 0; x <= this.textALength + 1; x++) {
          AlignmentNode node = this.getNode(x, y);
          if (node != null) {
            String nodeText;
            if (i == 0) {
              if (node.getReachedMainDiagonal()) {
                nodeText = "+" + formatNumber(node.getPenalty());
              } else {
                nodeText = "-" + formatNumber(node.getPenalty());
              }
            } else {
              if (i == 1) {
                nodeText = " " + formatNumber(node.getInsertXPenalty());
              } else {
                if (i == 2) {
                  nodeText = " " + formatNumber(node.getInsertYPenalty());
                } else {
                  nodeText = " " + formatNumber(Math.abs(getDistanceFromDiagonal(x, y)));
                }
              }
            }
            text += nodeText;
            empty = false;
            bestText = text;
          } else {
            text += "     ";
          }
        }
        this.logger.log(bestText);
      }
    }
  }

  private String formatNumber(double number) {
    int rounded = (int)(number * 10 + 0.5);
    String result = "";
    if (rounded >= 1000) {
      result += "BIG";
    } else {
      result += rounded;
    }
    while (result.length() < 4) {
      result += " ";
    }
    return result;
  }

  private void putNode(AlignmentNode node) {
    int x = node.getX();
    int y = node.getY();

    double currentPenalty = node.getPenalty();

    int numQueryPositionsChecked = x;
    int numAlignedBasepairs = Math.min(Math.abs(x - this.startX), Math.abs(y - this.startY));

    double estimatedTotalPenalty = this.estimateOverallPenalty(node);

    // Any node with penalty lower than what we're processing deserves to be processed before any less-important nodes
    if (estimatedTotalPenalty < this.activePenalty) {
      // This can happen if there is a node for which we already found the lowest penalty path ending at that node, and we're checking for other paths ending with an indel at that node
      estimatedTotalPenalty = this.activePenalty;
    }

    List<AlignmentNode> nodesWithThisPenalty = this.prioritizedNodes.get(estimatedTotalPenalty);

    if (nodesWithThisPenalty == null) {
      nodesWithThisPenalty = new ArrayList<AlignmentNode>();
      this.prioritizedNodes.put(estimatedTotalPenalty, nodesWithThisPenalty);
      this.priorities.add(estimatedTotalPenalty);
    }

    nodesWithThisPenalty.add(node);
    this.saveNode(node);
  }

  private double estimateOverallPenalty(AlignmentNode node) {
    if (!this.alignmentAnalysis.confidentAboutBestOffset) {
      return node.getPenalty();
    }

    int signedDistanceFromDiagonal = this.getSignedDistanceFromDiagonal(node.getX(), node.getY());

    if (node.getReachedMainDiagonal()) {

      // If we've travelled further from the main diagonal than expected, then we don't need to keep searching here
      if (signedDistanceFromDiagonal * this.stepDelta > 0) {
        double insertionExtensionPenalty = Math.abs(signedDistanceFromDiagonal * this.parameters.InsertionExtension_Penalty);
        if (insertionExtensionPenalty > this.alignmentAnalysis.maxInsertionExtensionPenalty) {
          return disallowed;
        }
      } else {
        double deletionExtensionPenalty = Math.abs(signedDistanceFromDiagonal * this.parameters.DeletionExtension_Penalty);
        if (deletionExtensionPenalty > this.alignmentAnalysis.maxDeletionExtensionPenalty) {
          return disallowed;
        }
      }

      if (node.getReachedOtherDiagonal()) {
        return node.getPenalty();
      } else {
        double indelPenalty = Math.min(this.parameters.InsertionStart_Penalty + this.parameters.InsertionExtension_Penalty, this.parameters.DeletionStart_Penalty + this.parameters.DeletionExtension_Penalty);
        return node.getPenalty() + indelPenalty;
      }
    }

    // If we haven't reached the main diagonal then we still have to get there
    if (signedDistanceFromDiagonal * this.stepDelta < 0) {
      double insertionExtensionPenalty = Math.abs(signedDistanceFromDiagonal * this.parameters.InsertionExtension_Penalty);
      if (insertionExtensionPenalty > this.alignmentAnalysis.maxInsertionExtensionPenalty) {
        return disallowed;
      }
      double insertionStartPenalty = Math.min(this.parameters.InsertionStart_Penalty, node.getInsertXPenalty() - node.getPenalty());
      return node.getPenalty() + insertionStartPenalty + insertionExtensionPenalty;
    } else {
      double deletionExtensionPenalty = Math.abs(signedDistanceFromDiagonal * this.parameters.DeletionExtension_Penalty);
      if (deletionExtensionPenalty > this.alignmentAnalysis.maxDeletionExtensionPenalty) {
        return disallowed;
      }
      double deletionStartPenalty = Math.min(this.parameters.DeletionStart_Penalty, node.getInsertYPenalty() - node.getPenalty());
      return node.getPenalty() + deletionStartPenalty + deletionExtensionPenalty;
    }
  }

  private void saveNode(AlignmentNode node) {
    int x = node.getX();
    int y = node.getY();
    if (x < 0 || y < 0)
      return;
    while (this.locatedNodes.size() <= x) {
      this.locatedNodes.add(new ArrayList<AlignmentNode>());
    }
    List<AlignmentNode> diagonal = this.locatedNodes.get(x);
    int encodedXY = (y - x) * 2;
    if (encodedXY < 0)
      encodedXY = -encodedXY - 1;
    while (diagonal.size() <= encodedXY) {
      diagonal.add(null);
    }
    diagonal.set(encodedXY, node);
  }

  private AlignmentNode getNode(int x, int y) {
    if (this.locatedNodes.size() <= x) {
      return null;
    }
    List<AlignmentNode> diagonal = this.locatedNodes.get(x);
    int encodedXY = (y - x) * 2;
    if (encodedXY < 0)
      encodedXY = -encodedXY - 1;
    if (encodedXY >= diagonal.size()) {
      return null;
    }
    return diagonal.get(encodedXY);
  }

  private void update(int x, int y) {
    if (x <= 0 || x > this.textALength)
      return;
    if (y <= 0 || y > this.textBLength)
      return;
    /*if (y > this.textBLength) {
      this.logger.log("update at " + x + "," + y + " with textALength = " + textALength + " textBLength = " + textBLength);
      String n = null;
      this.logger.log(n.toString());
    }*/
    //this.logger.log("update at " + x + "," + y);

    AlignmentNode node = this.computeUpdated(x, y);
    if (node != null) {
      this.putNode(node);
    }
  }

  private AlignmentNode computeUpdated(int x, int y) {
    //this.logger.log("computeUpdated at " + x + "," + y);
    AlignmentNode existing = this.getNode(x, y);
    AlignmentNode left = this.getNode(x - this.stepDelta, y);
    AlignmentNode up = this.getNode(x, y - this.stepDelta);
    AlignmentNode diagonal = this.getNode(x - this.stepDelta, y - this.stepDelta);


    double insertXPenalty, insertYPenalty, overlayPenalty, newOverlayPenalty;
    insertXPenalty = insertYPenalty = overlayPenalty = newOverlayPenalty = disallowed;

    if (diagonal != null) {
      byte a = this.getEncodedCharA(x - 1);
      byte b = this.getEncodedCharB(y - 1);
      newOverlayPenalty = Basepairs.getPenalty(a, b, this.parameters);
      overlayPenalty = diagonal.getPenalty() + newOverlayPenalty;
    }

    if (left != null) {
      if (y == goalY && mayQueryExtendPastEndOfReference) {
        // reached the end of the reference, so insertions are treated as unaligned instead
        insertXPenalty = left.getPenalty() + parameters.UnalignedPenalty;
      } else {
        boolean newInsertionAllowed = true;
        if (newInsertionAllowed) {
          int prevAIndex = x - 1 - this.stepDelta;
          int prevBIndex = y - 1;
          if (prevAIndex >= 0 && prevAIndex < this.textALength && prevBIndex >= 0 && prevBIndex < this.textBLength) {
            byte prevA = this.getEncodedCharA(prevAIndex);
            byte prevB = this.getEncodedCharB(prevBIndex);
            if (!Basepairs.canMatch(prevA, prevB)) {
              // It's wasteful to check for an insertion right after two mismatched basepairs
              // We should be able to shift the indel one position earlier and not get a worse score
              newInsertionAllowed = false;
            }
          }
        }
        if (newInsertionAllowed) {
          int nextAIndex = x - 1;
          int nextBIndex = y - 1 + this.stepDelta;
          if (nextAIndex >= 0 && nextAIndex < this.textALength && nextBIndex >= 0 && nextBIndex < this.textBLength) {
            byte nextA = this.getEncodedCharA(nextAIndex);
            byte nextB = this.getEncodedCharB(nextBIndex);
            if (Basepairs.getPenalty(nextA, nextB, this.parameters) == 0) {
              // It's wasteful to check for an insertion right before two matching basepairs
              // We should be able to shift the indel one position later and not get a worse score
              newInsertionAllowed = false;
            } else {
              if (Basepairs.isFullyAmbiguous(nextA) || Basepairs.isFullyAmbiguous(nextB)) {
                // It's wasteful to check for an insertion right before fully ambiguous basepairs ("N")
                // We should be able to shift the indel one position later and not get a worse score
                newInsertionAllowed = false;
              }
            }
          }
        }
        double newInsertXPenalty;
        if (newInsertionAllowed)
          newInsertXPenalty = left.getPenalty() + this.parameters.InsertionStart_Penalty + this.parameters.InsertionExtension_Penalty;
        else
          newInsertXPenalty = disallowed;
        double extendInsertXPenalty = left.getInsertXPenalty() + this.parameters.InsertionExtension_Penalty;
        insertXPenalty = Math.min(extendInsertXPenalty, newInsertXPenalty);
      }
    }

    if (up != null) {
      boolean newInsertionAllowed = true;
      if (newInsertionAllowed) {
        int prevAIndex = x - 1;
        int prevBIndex = y - 1 - this.stepDelta;
        if (prevAIndex >= 0 && prevAIndex < this.textALength && prevBIndex >= 0 && prevBIndex < this.textBLength) {
          byte prevA = this.getEncodedCharA(prevAIndex);
          byte prevB = this.getEncodedCharB(prevBIndex);
          if (!Basepairs.canMatch(prevA, prevB)) {
            newInsertionAllowed = false;
          }
        }
      }
      if (newInsertionAllowed) {
        int nextAIndex = x - 1 + this.stepDelta;
        int nextBIndex = y - 1;
        if (nextAIndex >= 0 && nextAIndex < this.textALength && nextBIndex >= 0 && nextBIndex < this.textBLength) {
          byte nextA = this.getEncodedCharA(nextAIndex);
          byte nextB = this.getEncodedCharB(nextBIndex);
          if (Basepairs.getPenalty(nextA, nextB, this.parameters) == 0) {
            newInsertionAllowed = false;
          } else {
            if (Basepairs.isFullyAmbiguous(nextA) || Basepairs.isFullyAmbiguous(nextB)) {
              // It's wasteful to check for an insertion right before fully ambiguous basepairs ("N")
              // We should be able to shift the indel one position later and not get a worse score
              newInsertionAllowed = false;
            }
          }
        }
      }
      double newInsertYPenalty;
      if (newInsertionAllowed)
        newInsertYPenalty = up.getPenalty() + this.parameters.DeletionStart_Penalty + this.parameters.DeletionExtension_Penalty;
      else
        newInsertYPenalty = disallowed;
      double extendInsertYPenalty = up.getInsertYPenalty() + this.parameters.DeletionExtension_Penalty;
      insertYPenalty = Math.min(extendInsertYPenalty, newInsertYPenalty);
    }

    double bestPenalty = Math.min(Math.min(overlayPenalty, insertXPenalty), insertYPenalty);

    AlignmentNode newNode = null;
    if (existing == null || bestPenalty < existing.getPenalty() || insertXPenalty < existing.getInsertXPenalty() || insertYPenalty < existing.getInsertYPenalty()) {

      boolean reachedMainDiagonal = false;
      boolean reachedOtherDiagonal = false;
      if (bestPenalty == disallowed) {
        reachedMainDiagonal = false;
        reachedOtherDiagonal = false;
      } else {
        if (bestPenalty == overlayPenalty) {
          reachedMainDiagonal = diagonal.getReachedMainDiagonal();
          reachedOtherDiagonal = diagonal.getReachedOtherDiagonal();
        } else {
          if (bestPenalty == insertXPenalty) {
            reachedMainDiagonal = left.getReachedMainDiagonal();
            reachedOtherDiagonal = left.getReachedOtherDiagonal();
          } else {
            reachedMainDiagonal = up.getReachedMainDiagonal();
            reachedOtherDiagonal = up.getReachedOtherDiagonal();
          }
        }

        if (this.getDistanceFromDiagonal(x, y) == 0) {
          reachedMainDiagonal = true;
        } else {
          reachedOtherDiagonal = true;
        }
      }

      newNode = new AlignmentNode(x, y, bestPenalty, insertXPenalty, insertYPenalty, reachedMainDiagonal, reachedOtherDiagonal);
    }
    /*if (this.logger.getEnabled()) {
      if (existing == null) {
        this.logger.log("existing node at " + x + ", " + y + " is null, new node penalty = " + bestPenalty);
      } else {
        this.logger.log("existing node at " + x + ", " + y + " penalty = " + existing.getPenalty() + ", new node penalty = " + bestPenalty);
      }
    }*/
    return newNode;
  }

  // explores paths of length 1 leading out of (x,y)
  public void explore(int x, int y) {
    /*if (this.logger.getEnabled()) {
      this.logger.log("explore " + x + " " + y);
    }*/
    this.update(x + this.stepDelta, y);
    this.update(x, y + this.stepDelta);
    this.update(x + this.stepDelta, y + this.stepDelta);
  }

  private void reverseList(List<AlignedBlock> list) {
    int middle = list.size() / 2;
    for (int i = 0; i < middle; i++) {
      int j = list.size() - i - 1;
      AlignedBlock left = list.get(i);
      list.set(i, list.get(j));
      list.set(j, left);
    }
  }

  private int getMapKey(int x, int y) {
    return y * (this.textALength + 2) + x;
  }

  // use the previously decompressed value so we don't have to decompress it again
  private byte getEncodedCharA(int index) {
    return this.queryEncodedChars[index];
  }

  // use the previously decompressed value so we don't have to decompress it again
  private byte getEncodedCharB(int index) {
    return this.referenceEncodedChars[index];
  }

  private int getSignedDistanceFromDiagonal(int x, int y) {
    return x - y - this.diagonal;
  }
  private int getDistanceFromDiagonal(int x, int y) {
    return Math.abs(getSignedDistanceFromDiagonal(x, y));
  }
  private byte[] sequenceToEncodedChars(Sequence sequence, int startIndex, int endIndex) {
    int length = endIndex - startIndex;
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++) {
      result[i] = sequence.encodedCharAt(startIndex + i);
    }
    return result;
  }

  // a large number that will be worse than any other penalty
  private static double disallowed = 1000000.0;

  AlignmentParameters parameters;
  Logger logger;
  Sequence query;
  Sequence reference;
  byte[] queryEncodedChars;
  byte[] referenceEncodedChars;
  int startIndexB;
  int endIndexB;
  int startIndexA;
  int endIndexA;
  int textALength;
  int textBLength;
  int startX, startY, goalX, goalY;
  int diagonal;
  double maxInterestingPenalty;
  boolean mayQueryExtendPastEndOfReference;

  HashMap<Double, List<AlignmentNode>> prioritizedNodes;
  PriorityQueue<Double> priorities;

  // Map<X, encoded X-Y, AlignmentNode>
  List<List<AlignmentNode>> locatedNodes;
  double activePenalty;
  int stepDelta;
  boolean searchReverse;
  AlignmentAnalysis alignmentAnalysis;
}
