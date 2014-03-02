package models

import org.apache.mahout.cf.taste.common.Weighting;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.impl.similarity.{EuclideanDistanceSimilarity}


class EuclideanDistanceOrExactValue(dataModel: DataModel, weighting: Weighting) extends AbstractCustomSimilarity(dataModel, weighting, false) {


  override def computeResult(n: Int, sumXY: Double, sumX2: Double, sumY2: Double, sumXYdiff2: Double): Double = {
    return 1.0 / (1.0 + Math.sqrt(sumXYdiff2) / Math.sqrt(n));
  }
}
