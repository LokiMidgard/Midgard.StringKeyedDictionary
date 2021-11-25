using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Midgard.Collections.StringKeyedDictionary.Tests
{
    [TestClass]
    public class UnitTest1
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
        public void TestContains()
        {
            var dictionary = new StringKeyDictionary<int>();
            dictionary.Add("Hallo", 1);
            dictionary.Add("Welt", 2);

            Assert.AreEqual(true, dictionary.ContainsKey("Hallo Welt"[..5]));
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
    }
}