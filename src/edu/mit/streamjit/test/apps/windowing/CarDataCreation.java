package edu.mit.streamjit.test.apps.windowing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class CarDataCreation {
	public static final long serialVersionUID = 1L;
	public static Integer[] speeds;
	public static Double[] distances;

	public static Random rand = new Random();

	public volatile static int isRunning = 0;

	static File file = new File ("cardata.in");
	static BufferedWriter out;
	
	public static void main(String[] args) throws InterruptedException, IOException{
		out = new BufferedWriter(new FileWriter(file)); 
		CarSource.create(2);
		CarSource.run();
		out.close();
	}
	
	public static void writeFile(String l) 
			  throws IOException
			  {
			    out.write(l);
			    out.newLine();
			  }

	public static class CarSource {
		public CarSource(int numOfCars) {
			speeds = new Integer[numOfCars];
			distances = new Double[numOfCars];
			Arrays.fill(speeds, 50);
			Arrays.fill(distances, 0d);
		}

		public static CarSource create(int cars) {
			return new CarSource(cars);
		}

		public static void run() throws InterruptedException, IOException {

			while (isRunning<=100) {
				Thread.sleep(100);
				for (int carId = 0; carId < speeds.length; carId++) {
					if (rand.nextBoolean()) {
						speeds[carId] = Math.min(100, speeds[carId] + 5);
					} else {
						speeds[carId] = Math.max(0, speeds[carId] - 5);
					}
					distances[carId] += speeds[carId] / 3.6d;
					Tuple4<Integer, Integer, Double, Long> record = new Tuple4<>(carId,
							speeds[carId], distances[carId], System.currentTimeMillis());
					isRunning++;
					writeFile(record.toString());
				}
			}
		}
	}
		
}
