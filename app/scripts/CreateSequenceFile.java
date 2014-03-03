package scripts;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.WeightedDistanceMeasure;
import org.apache.mahout.common.distance.WeightedEuclideanDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateSequenceFile {

    /*
     * (trade, postcode, size_of_business, years_start_business)
     */
    public static final double[][] points = {
            {1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1, 1},
            {1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1, 1},
            {1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1, 1},
            {3, 4, 5, 6}, {7, 8, 9, 10}, {1, 1, 1, 1},{1,2,1,2},
            {1,1,3,2},
            {1,3,2,2},
            {1,3,1,1},
            {3,1,2,1},
            {3,1,1,3},
            {2,1,1,1},
            {2,1,1,2},
            {1,2,1,3},
            {2,1,3,1},
            {1,1,1,3},
            {3,1,1,3},
            {1,2,1,3},
            {2,1,3,3},
            {1,2,1,1},
            {1,3,1,3},
            {2,3,1,1},
            {1,3,1,1},
            {1,1,3,3},
            {2,1,2,2},
            {1,1,1,1},
            {1,1,2,3},
            {2,1,1,2},
            {2,1,1,3},
            {1,1,1,1},
            {1,2,3,3},
            {2,1,1,1},
            {1,3,2,3},
            {2,2,3,2},
            {2,1,1,2},
            {2,3,1,1},
            {1,3,1,1},
            {3,3,1,3},
            {2,1,2,1},
            {1,2,3,3},
            {1,1,1,1},
            {1,2,1,2},
            {1,1,1,2},
            {1,1,3,1},
            {1,1,1,1},
            {1,1,2,3},
            {1,2,1,3},
            {2,1,1,2}};

    public static final double[] weight = {10, 5, 1, 1};

    public static void writePointsToFile(List<Vector> points,
                                         String fileName,
                                         FileSystem fs,
                                         Configuration conf) throws IOException {
        Path path = new Path(fileName);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
                path, LongWritable.class, VectorWritable.class);
        long recNum = 0;
        VectorWritable vec = new VectorWritable();
        for (Vector point : points) {
            vec.set(point);
            writer.append(new LongWritable(recNum++), vec);
        }
        writer.close();
    }

    public static List<Vector> getPoints(double[][] raw) {
        List<Vector> points = new ArrayList<Vector>();
        for (int i = 0; i < raw.length; i++) {
            double[] fr = raw[i];
            NamedVector vec = new NamedVector(
                    new RandomAccessSparseVector(fr.length),
                    "NAME"+i
            );
            vec.assign(fr);
            points.add(vec);
        }
        return points;
    }

    public static void run() throws Exception {

        int k = 8;

        List<Vector> vectors = getPoints(points);

        File testData = new File("clustering/testdata");
        testData.delete();
        if (!testData.exists()) {
            testData.mkdir();
        }
        testData = new File("clustering/testdata/points");
        testData.delete();
        if (!testData.exists()) {
            testData.mkdir();
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        writePointsToFile(vectors, "clustering/testdata/points/file1", fs, conf);

        Path path = new Path("clustering/testdata/clusters/part-00000");
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Kluster.class);


      // adding weights

        WeightedDistanceMeasure distanceMeasure = new WeightedEuclideanDistanceMeasure();
        NamedVector weightvector = new NamedVector(
                new RandomAccessSparseVector(weight.length),
                "weight"
        );
        weightvector.assign(weight);

        distanceMeasure.setWeights(weightvector);


       // ---------


        for (int i = 0; i < k; i++) {
            Vector vec = vectors.get(i);
            Kluster cluster = new Kluster(vec, i, distanceMeasure);
            writer.append(new Text(cluster.getIdentifier()), cluster);
        }
        writer.close();

        KMeansDriver.run(conf,
                new Path("clustering/testdata/points"),
                new Path("clustering/testdata/clusters"),
                new Path("clustering/output"),
                0.001,
                10,
                true,
                0,
                true);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,
                new Path("clustering/output/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-0"), conf);

        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
        while (reader.next(key, value)) {
            NamedVector nVec = (NamedVector)value.getVector();
            System.out.println("ID: "+nVec+ "  "+value.toString() + " belongs to cluster " + key.toString());
        }
        reader.close();


        ClusterDumper clusterDumper = new ClusterDumper(new Path("clustering/output",
                "clusters-3-final"), new Path("clustering/output/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-0"));
        clusterDumper.printClusters(null);

        Iterable<ClusterWritable>  clusterWritable = new SequenceFileDirValueIterable<ClusterWritable>(new Path("clustering/output/clusters-10-final",
               "part-*"), PathType.GLOB, conf);


        Map<Integer, Vector> centers = new HashMap<>();
        for(ClusterWritable writable: clusterWritable){
            Cluster cluster = writable.getValue();
            System.out.println(cluster.getCenter());
            centers.put(cluster.getId(), cluster.getCenter());
        }


        // Find cluster for new point:

        double[] newPoint={3,4,5,6};
        NamedVector newPointVector = new NamedVector(
                new RandomAccessSparseVector(newPoint.length),
                "weight"
        );
        newPointVector.assign(newPoint);

        double currentMinDistance = 10;
        int clusterIdBelong = 0;
        for(Integer vecKey:centers.keySet()){
            Vector vector = centers.get(vecKey);
            double distance = distanceMeasure.distance(vector, newPointVector);
            if(distance<currentMinDistance){
                clusterIdBelong = vecKey;
                currentMinDistance = distance;
            }
        }

        System.out.println("New point belongs to cluster" +clusterIdBelong+ " with a distance of "+ currentMinDistance);


        // ---------------

    }

}
