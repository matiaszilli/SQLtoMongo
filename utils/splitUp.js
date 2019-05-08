
/**
 * Split a given array "array" into "n" chunks.
 *
 * @param {Array} array
 * @param {Number} n
 * @return {Array}
 */

module.exports = function splitUp(array, n) {
    var rest = array.length % n, // how much to divide
        restUsed = rest, // to keep track of the division over the elements
        partLength = Math.floor(array.length / n),
        result = [];

    for(var i = 0; i < array.length; i += partLength) {
        var end = partLength + i,
            add = false;

        if(rest !== 0 && restUsed) { // should add one element for the division
            end++;
            restUsed--; // we've used one division element now
            add = true;
        }

        result.push(array.slice(i, end)); // part of the array

        if(add) {
            i++; // also increment i in the case we added an extra element for division
        }
    }

    return result;
}