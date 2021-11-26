using Microsoft.VisualStudio.TestTools.UnitTesting;

using KeyValuePair = System.Collections.Generic.KeyValuePair;

namespace Midgard.Collections.StringKeyedDictionary.Tests
{
    [TestClass]
    public class UnitTestDictionary
    {
        [TestMethod]
        public void TestCount()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);
            Assert.AreEqual(2, dictionary.Count);
        }


        [TestMethod]
        public void TestBulkInsert()
        {
            var dictionary = new StringKeyDictionary<int>();
            for (int i = 0; i < 1000_000; i++)
            {
                dictionary.Add("Hallo" + i, i);
            }
            Assert.AreEqual(1000_000, dictionary.Count);
        }


        [TestMethod]
        public void TestContainsKey()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);
            Assert.AreEqual(true, dictionary.ContainsKey("Hallo Welt"[..5]));
        }
        [TestMethod]
        public void TestContainsValue()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);
            Assert.AreEqual(true, dictionary.ContainsValue(2));
        }
        [TestMethod]
        public void TestContainsKeyNegativ()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);
            Assert.AreEqual(false, dictionary.ContainsKey("Hallo Welt"[1..6]));
        }
        [TestMethod]
        public void TestContainsValueNegative()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);
            Assert.AreEqual(false, dictionary.ContainsValue(3));
        }

        [TestMethod]
        public void TestRetriv()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);

            Assert.AreEqual(1, dictionary["Hallo Welt"[..5]]);
            Assert.AreEqual(2, dictionary["Hallo Welt"[6..]]);

        }

        [TestMethod]
        public void TestUpdate()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);
            dictionary["Welt"] = 3;

            Assert.AreEqual(3, dictionary["Hallo Welt"[6..]]);
        }

        [TestMethod]
        public void TestKeysCollection()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);

            var keys = dictionary.Keys;

            CollectionAssert.AreEquivalent(new[] { "Hallo", "Welt" }, keys);
        }

        [TestMethod]
        public void TestValuesCollection()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);

            var values = dictionary.Values;

            CollectionAssert.AreEquivalent(new[] { 2, 1 }, values);
        }

        [TestMethod]
        public void TestEnumeration()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);

            CollectionAssert.AreEquivalent(new[] { KeyValuePair.Create("Hallo", 1), KeyValuePair.Create("Welt", 2) }, dictionary);
        }
    }
}