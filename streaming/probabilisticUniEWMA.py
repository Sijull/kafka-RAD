import numpy as np
import math
from scipy.stats import norm

class probabilisticUniEWMA:
    var = None
    mean = None
    n = 0
    alpha, beta = 0, 0

    def __getVar(self, X):
        """
        @param: X is dimension (n x 1) where n: number of samples
        @return: var is the variance
        """
        var = np.var(X, ddof=1)
        return var

    def __estimateInitParameters(self, n):
        """
        @param: n, number of samples in static setting
        @return: (alpha, beta) is a tuple of alpha, beta
        """
        C_var = 2.0 / (math.pow(n, 2) + 6)
        alpha = 1 - C_var
        beta = 1 - alpha
        return alpha, beta

    def __updateVariance(self, alpha, beta, var, z_t, mean):
        """
        @param: alpha, beta are parameters of the model
        @param: var is old variance, z_t as new data point, mean is the mean
        @return: var_tplus1 is updated variance
        """
        var_tplus1 = alpha * var + beta * (z_t - mean)**2
        return var_tplus1

    def __updateMean(self, mean, x):
        mean_tplus1 = ((self.n * mean) + x) / (self.n + 1)
        self.n = self.n + 1
        return mean_tplus1

    def init (self, X):
        self.n = len(X)
        self.var = self.__getVar(X)  
        self.alpha, self.beta = self.__estimateInitParameters(self.n)  
        self.mean = np.mean(X)

    def update(self, z_t):  
        self.var = self.__updateVariance(self.alpha, self.beta, self.var, z_t, self.mean)

    def predict(self, x):
        """
        @param: x is the current data point
        @return: score of anomaly
        """
        eps = 1e-8
        score = norm.pdf(x, loc=self.mean, scale=np.sqrt(abs(self.var + eps)))
        self.mean = self.__updateMean(self.mean, x) # update mean
        return score

    def bulkPredict(self, Z):
        """
        @param: Z is the list of data points
        @return: score of anomaly
        """
        scores = []

        for ind in range(1, len(Z)):
            cur = Z[ind-1]
            next = Z[ind]
            self.update(cur)
            score = self.predict(next)
            scores.append(score)
        return scores

    def getCurrentVariance(self):
        """
        @return: variance
        """
        return self.var

    def getOriginalVariance(self, X):
        """
        @return: original variance
        """
        return self.__getVar(X)