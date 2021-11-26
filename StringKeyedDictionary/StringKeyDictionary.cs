// This class is a slightly modified variant from System.Collections.Hashtable
// It uses a string Key that can also use ReadonlySpan<char> and the Value is 
// Generic paramter. The changes are Licensed under MIT the original File was
// also Licensed under MIT (original statement below)

// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Threading;


namespace Midgard.Collections
{


    // The Hashtable class represents a dictionary of associated keys and values
    // with constant lookup time.
    //
    // Objects used as keys in a hashtable must implement the GetHashCode
    // and Equals methods (or they can rely on the default implementations
    // inherited from Object if key equality is simply reference
    // equality). Furthermore, the GetHashCode and Equals methods of
    // a key object must produce the same results given the same parameters for the
    // entire time the key is present in the hashtable. In practical terms, this
    // means that key objects should be immutable, at least for the time they are
    // used as keys in a hashtable.
    //
    // When entries are added to a hashtable, they are placed into
    // buckets based on the hashcode of their keys. Subsequent lookups of
    // keys will use the hashcode of the keys to only search a particular bucket,
    // thus substantially reducing the number of key comparisons required to find
    // an entry. A hashtable's maximum load factor, which can be specified
    // when the hashtable is instantiated, determines the maximum ratio of
    // hashtable entries to hashtable buckets. Smaller load factors cause faster
    // average lookup times at the cost of increased memory consumption. The
    // default maximum load factor of 1.0 generally provides the best balance
    // between speed and size. As entries are added to a hashtable, the hashtable's
    // actual load factor increases, and when the actual load factor reaches the
    // maximum load factor value, the number of buckets in the hashtable is
    // automatically increased by approximately a factor of two (to be precise, the
    // number of hashtable buckets is increased to the smallest prime number that
    // is larger than twice the current number of hashtable buckets).
    //
    // Each object provides their own hash function, accessed by calling
    // GetHashCode().  However, one can write their own object
    // implementing IEqualityComparer and pass it to a constructor on
    // the Hashtable.  That hash function (and the equals method on the
    // IEqualityComparer) would be used for all objects in the table.
    //
    [DebuggerTypeProxy(typeof(StringKeyDictionary<>.HashtableDebugView))]
    [DebuggerDisplay("Count = {Count}")]
    public class StringKeyDictionary<T> : IDictionary<string, T>, ICloneable, ICollection
    {
        /*
          This Hashtable uses double hashing.  There are hashsize buckets in the
          table, and each bucket can contain 0 or 1 element.  We use a bit to mark
          whether there's been a collision when we inserted multiple elements
          (ie, an inserted item was hashed at least a second time and we probed
          this bucket, but it was already in use).  Using the collision bit, we
          can terminate lookups & removes for elements that aren't in the hash
          table more quickly.  We steal the most significant bit from the hash code
          to store the collision bit.
 
          Our hash function is of the following form:
 
          h(key, n) = h1(key) + n*h2(key)
 
          where n is the number of times we've hit a collided bucket and rehashed
          (on this particular lookup).  Here are our hash functions:
 
          h1(key) = GetHash(key);  // default implementation calls key.GetHashCode();
          h2(key) = 1 + (((h1(key) >> 5) + 1) % (hashsize - 1));
 
          The h1 can return any number.  h2 must return a number between 1 and
          hashsize - 1 that is relatively prime to hashsize (not a problem if
          hashsize is prime).  (Knuth's Art of Computer Programming, Vol. 3, p. 528-9)
          If this is true, then we are guaranteed to visit every bucket in exactly
          hashsize probes, since the least common multiple of hashsize and h2(key)
          will be hashsize * h2(key).  (This is the first number where adding h2 to
          h1 mod hashsize will be 0 and we will search the same bucket twice).
 
          We previously used a different h2(key, n) that was not constant.  That is a
          horrifically bad idea, unless you can prove that series will never produce
          any identical numbers that overlap when you mod them by hashsize, for all
          subranges from i to i+hashsize, for all i.  It's not worth investigating,
          since there was no clear benefit from using that hash function, and it was
          broken.
 
          For efficiency reasons, we've implemented this by storing h1 and h2 in a
          temporary, and setting a variable called seed equal to h1.  We do a probe,
          and if we collided, we simply add h2 to seed each time through the loop.
 
          A good test for h2() is to subclass Hashtable, provide your own implementation
          of GetHash() that returns a constant, then add many items to the hash table.
          Make sure Count equals the number of items you inserted.
 
          Note that when we remove an item from the hash table, we set the key
          equal to buckets, if there was a collision in this bucket.  Otherwise
          we'd either wipe out the collision bit, or we'd still have an item in
          the hash table.
 
           --
        */

        private const int InitialSize = 3;


        // Deleted entries have their key set to buckets

        // The hash table data.
        // This cannot be serialized
        private struct bucket
        {
            public object? key;
            public T? val;
            public int hash_coll;   // Store hash code; sign bit means there was a collision.
        }

        private bucket[] _buckets;

        // The total number of entries in the hash table.
        private int _count;

        // The total number of collision bits set in the hashtable
        private int _occupancy;

        private int _loadsize;
        private float _loadFactor;

        private volatile int _version;
        private volatile bool _isWriterInProgress;

        private KeyCollection? _keys;
        private ValueCollection? _values;


        // Note: this constructor is a bogus constructor that does nothing
        // and is for use only with SyncHashtable.
        private StringKeyDictionary(bool trash)
        {
            _buckets = null!;
        }

        // Constructs a new hashtable. The hashtable is created with an initial
        // capacity of zero and a load factor of 1.0.
        public StringKeyDictionary() : this(0, 1.0f)
        {
        }

        // Constructs a new hashtable with the given initial capacity and a load
        // factor of 1.0. The capacity argument serves as an indication of
        // the number of entries the hashtable will contain. When this number (or
        // an approximation) is known, specifying it in the constructor can
        // eliminate a number of resizing operations that would otherwise be
        // performed when elements are added to the hashtable.
        //
        public StringKeyDictionary(int capacity) : this(capacity, 1.0f)
        {
        }

        // Constructs a new hashtable with the given initial capacity and load
        // factor. The capacity argument serves as an indication of the
        // number of entries the hashtable will contain. When this number (or an
        // approximation) is known, specifying it in the constructor can eliminate
        // a number of resizing operations that would otherwise be performed when
        // elements are added to the hashtable. The loadFactor argument
        // indicates the maximum ratio of hashtable entries to hashtable buckets.
        // Smaller load factors cause faster average lookup times at the cost of
        // increased memory consumption. A load factor of 1.0 generally provides
        // the best balance between speed and size.
        //
        public StringKeyDictionary(int capacity, float loadFactor)
        {
            if (capacity < 0)
                throw new ArgumentOutOfRangeException(nameof(capacity), "Capacaty must be positiv");
            if (!(loadFactor >= 0.1f && loadFactor <= 1.0f))
                throw new ArgumentOutOfRangeException(nameof(loadFactor), "must be between 0.1f and 1.0f");

            // Based on perf work, .72 is the optimal load factor for this table.
            _loadFactor = 0.72f * loadFactor;

            double rawsize = capacity / _loadFactor;
            if (rawsize > int.MaxValue)
                throw new ArgumentException("To big", nameof(capacity));

            // Avoid awfully small sizes
            int hashsize = rawsize > InitialSize ? HashHelpers.GetPrime((int)rawsize) : InitialSize;
            _buckets = new bucket[hashsize];

            _loadsize = (int)(_loadFactor * hashsize);
            _isWriterInProgress = false;
            // Based on the current algorithm, loadsize must be less than hashsize.
            Debug.Assert(_loadsize < hashsize, "Invalid hashtable loadsize!");
        }





        // Constructs a new hashtable containing a copy of the entries in the given
        // dictionary. The hashtable is created with a load factor of 1.0.
        //
        public StringKeyDictionary(IDictionary d) : this(d, 1.0f)
        {
        }




        public StringKeyDictionary(IDictionary d, float loadFactor)
            : this(d != null ? d.Count : 0, loadFactor)
        {
            if (d == null)
                throw new ArgumentNullException(nameof(d));

            IDictionaryEnumerator e = d.GetEnumerator();
            while (e.MoveNext())
                Add((string)e.Key, (T)e.Value!);

        }


        // ?InitHash? is basically an implementation of classic DoubleHashing (see http://en.wikipedia.org/wiki/Double_hashing)
        //
        // 1) The only ?correctness? requirement is that the ?increment? used to probe
        //    a. Be non-zero
        //    b. Be relatively prime to the table size ?hashSize?. (This is needed to insure you probe all entries in the table before you ?wrap? and visit entries already probed)
        // 2) Because we choose table sizes to be primes, we just need to insure that the increment is 0 < incr < hashSize
        //
        // Thus this function would work: Incr = 1 + (seed % (hashSize-1))
        //
        // While this works well for ?uniformly distributed? keys, in practice, non-uniformity is common.
        // In particular in practice we can see ?mostly sequential? where you get long clusters of keys that ?pack?.
        // To avoid bad behavior you want it to be the case that the increment is ?large? even for ?small? values (because small
        // values tend to happen more in practice). Thus we multiply ?seed? by a number that will make these small values
        // bigger (and not hurt large values). We picked HashPrime (101) because it was prime, and if ?hashSize-1? is not a multiple of HashPrime
        // (enforced in GetPrime), then incr has the potential of being every value from 1 to hashSize-1. The choice was largely arbitrary.
        //
        // Computes the hash function:  H(key, i) = h1(key) + i*h2(key, hashSize).
        // The out parameter seed is h1(key), while the out parameter
        // incr is h2(key, hashSize).  Callers of this function should
        // add incr each time through a loop.
        private uint InitHash(ReadOnlySpan<char> key, int hashsize, out uint seed, out uint incr)
        {
            // Hashcode must be positive.  Also, we must not use the sign bit, since
            // that is used for the collision bit.
            uint hashcode = (uint)GetHash(key) & 0x7FFFFFFF;
            seed = hashcode;
            // Restriction: incr MUST be between 1 and hashsize - 1, inclusive for
            // the modular arithmetic to work correctly.  This guarantees you'll
            // visit every bucket in the table exactly once within hashsize
            // iterations.  Violate this and it'll cause obscure bugs forever.
            // If you change this calculation for h2(key), update putEntry too!
            incr = 1 + seed * HashHelpers.HashPrime % ((uint)hashsize - 1);
            return hashcode;
        }

        // Adds an entry with the given key and value to this hashtable. An
        // ArgumentException is thrown if the key is null or if the key is already
        // present in the hashtable.
        //
        public virtual void Add(ReadOnlySpan<char> key, T value)
        {
            Insert(key, value, true);
        }

        // Removes all entries from this hashtable.
        public virtual void Clear()
        {
            Debug.Assert(!_isWriterInProgress, "Race condition detected in usages of Hashtable - multiple threads appear to be writing to a Hashtable instance simultaneously!  Don't do that - use Hashtable.Synchronized.");

            if (_count == 0 && _occupancy == 0)
                return;

            _isWriterInProgress = true;
            for (int i = 0; i < _buckets.Length; i++)
            {
                _buckets[i].hash_coll = 0;
                _buckets[i].key = null;
                _buckets[i].val = default!;
            }

            _count = 0;
            _occupancy = 0;
            UpdateVersion();
            _isWriterInProgress = false;
        }

        // Clone returns a virtually identical copy of this hash table.  This does
        // a shallow copy - the Objects in the table aren't cloned, only the references
        // to those Objects.
        public virtual object Clone()
        {
            bucket[] lbuckets = _buckets;
            StringKeyDictionary<T> ht = new StringKeyDictionary<T>(_count)
            {
                _version = _version,
                _loadFactor = _loadFactor,
                _count = 0
            };

            int bucket = lbuckets.Length;
            while (bucket > 0)
            {
                bucket--;
                object? keyv = lbuckets[bucket].key;
                if (keyv is string s)
                {
                    T? val = lbuckets[bucket].val;
                    Debug.Assert(val != null);
                    ht[s] = val;
                }
            }

            return ht;
        }

        // Checks if this hashtable contains an entry with the given key.  This is
        // an O(1) operation.
        //
        public virtual bool ContainsKey(ReadOnlySpan<char> key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            // Take a snapshot of buckets, in case another thread resizes table
            bucket[] lbuckets = _buckets;
            uint hashcode = InitHash(key, lbuckets.Length, out uint seed, out uint incr);
            int ntry = 0;

            bucket b;
            int bucketNumber = (int)(seed % (uint)lbuckets.Length);
            do
            {
                b = lbuckets[bucketNumber];
                if (b.key == null)
                {
                    return false;
                }
                if ((b.hash_coll & 0x7FFFFFFF) == hashcode &&
                    KeyEquals(b.key, key))
                    return true;
                bucketNumber = (int)((bucketNumber + incr) % (uint)lbuckets.Length);
            } while (b.hash_coll < 0 && ++ntry < lbuckets.Length);
            return false;
        }

        // Checks if this hashtable contains an entry with the given value. The
        // values of the entries of the hashtable are compared to the given value
        // using the Object.Equals method. This method performs a linear
        // search and is thus be substantially slower than the ContainsKey
        // method.
        //
        public virtual bool ContainsValue(T value)
        {
            if (value == null)
            {
                for (int i = _buckets.Length; --i >= 0;)
                {
                    if (_buckets[i].key != null && _buckets[i].key != _buckets && _buckets[i].val == null)
                        return true;
                }
            }
            else
            {
                for (int i = _buckets.Length; --i >= 0;)
                {
                    object? val = _buckets[i].val;
                    if (val != null && val.Equals(value))
                        return true;
                }
            }
            return false;
        }

        // Copies the keys of this hashtable to a given array starting at a given
        // index. This method is used by the implementation of the CopyTo method in
        // the KeyCollection class.
        private void CopyKeys(Array array, int arrayIndex)
        {
            Debug.Assert(array != null);
            Debug.Assert(array.Rank == 1);

            bucket[] lbuckets = _buckets;
            for (int i = lbuckets.Length; --i >= 0;)
            {
                object? keyv = lbuckets[i].key;
                if (keyv != null && keyv != _buckets)
                {
                    array.SetValue(keyv, arrayIndex++);
                }
            }
        }

        // Copies the keys of this hashtable to a given array starting at a given
        // index. This method is used by the implementation of the CopyTo method in
        // the KeyCollection class.
        private void CopyEntries(Array array, int arrayIndex)
        {
            Debug.Assert(array != null);
            Debug.Assert(array.Rank == 1);

            bucket[] lbuckets = _buckets;
            for (int i = lbuckets.Length; --i >= 0;)
            {
                object? keyv = lbuckets[i].key;
                if (keyv is string s)
                {
                    T? val = lbuckets[i].val;
                    Debug.Assert(val != null);
                    KeyValuePair<string, T> entry = KeyValuePair.Create(s, val);
                    array.SetValue(entry, arrayIndex++);
                }
            }
        }

        // Copies the values in this hash table to an array at
        // a given index.  Note that this only copies values, and not keys.
        public virtual void CopyTo(Array array, int arrayIndex)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));
            if (array.Rank != 1)
                throw new ArgumentException("Multidimensional array not supported", nameof(array));
            if (arrayIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(arrayIndex), "No negative index");
            if (array.Length - arrayIndex < Count)
                throw new ArgumentException("Array to small");

            CopyEntries(array, arrayIndex);
        }

        //// Copies the values in this Hashtable to an KeyValuePairs array.
        //// KeyValuePairs is different from Dictionary Entry in that it has special
        //// debugger attributes on its fields.

        internal virtual KeyValuePairs[] ToKeyValuePairsArray()
        {
            KeyValuePairs[] array = new KeyValuePairs[_count];
            int index = 0;
            bucket[] lbuckets = _buckets;
            for (int i = lbuckets.Length; --i >= 0;)
            {
                object? keyv = lbuckets[i].key;
                if (keyv != null && keyv != _buckets)
                {
                    array[index++] = new KeyValuePairs(keyv, lbuckets[i].val);
                }
            }

            return array;
        }

        // Copies the values of this hashtable to a given array starting at a given
        // index. This method is used by the implementation of the CopyTo method in
        // the ValueCollection class.
        private void CopyValues(Array array, int arrayIndex)
        {
            Debug.Assert(array != null);
            Debug.Assert(array.Rank == 1);

            bucket[] lbuckets = _buckets;
            for (int i = lbuckets.Length; --i >= 0;)
            {
                object? keyv = lbuckets[i].key;
                if (keyv != null && keyv != _buckets)
                {
                    array.SetValue(lbuckets[i].val, arrayIndex++);
                }
            }
        }

        // Returns the value associated with the given key. If an entry with the
        // given key is not found, the returned value is null.
        //
        public virtual T this[ReadOnlySpan<char> key]
        {
            get
            {
                if (TryGetValue(key, out var value))
                    return value;
                throw new KeyNotFoundException($"The key {key.ToString()} was not in this collection.");
            }

            set => Insert(key, value, false);
        }



        // Increases the bucket count of this hashtable. This method is called from
        // the Insert method when the actual load factor of the hashtable reaches
        // the upper limit specified when the hashtable was constructed. The number
        // of buckets in the hashtable is increased to the smallest prime number
        // that is larger than twice the current number of buckets, and the entries
        // in the hashtable are redistributed into the new buckets using the cached
        // hashcodes.
        private void expand()
        {
            int rawsize = HashHelpers.ExpandPrime(_buckets.Length);
            rehash(rawsize);
        }

        // We occasionally need to rehash the table to clean up the collision bits.
        private void rehash()
        {
            rehash(_buckets.Length);
        }

        private void UpdateVersion()
        {
            // Version might become negative when version is int.MaxValue, but the oddity will be still be correct.
            // So we don't need to special case this.
            _version++;
        }

        private void rehash(int newsize)
        {
            // reset occupancy
            _occupancy = 0;

            // Don't replace any internal state until we've finished adding to the
            // new bucket[].  This serves two purposes:
            //   1) Allow concurrent readers to see valid hashtable contents
            //      at all times
            //   2) Protect against an OutOfMemoryException while allocating this
            //      new bucket[].
            bucket[] newBuckets = new bucket[newsize];

            // rehash table into new buckets
            int nb;
            for (nb = 0; nb < _buckets.Length; nb++)
            {
                bucket oldb = _buckets[nb];
                if (oldb.key is string s)
                {
                    int hashcode = oldb.hash_coll & 0x7FFFFFFF;
                    Debug.Assert(oldb.val != null);
                    putEntry(newBuckets, s, oldb.val, hashcode);
                }
            }

            // New bucket[] is good to go - replace buckets and other internal state.
            _isWriterInProgress = true;
            _buckets = newBuckets;
            _loadsize = (int)(_loadFactor * newsize);
            UpdateVersion();
            _isWriterInProgress = false;
            // minimum size of hashtable is 3 now and maximum loadFactor is 0.72 now.
            Debug.Assert(_loadsize < newsize, "Our current implementation means this is not possible.");
        }

        // Returns an enumerator for this hashtable.
        // If modifications made to the hashtable while an enumeration is
        // in progress, the MoveNext and Current methods of the
        // enumerator will throw an exception.
        //
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        // Returns a dictionary enumerator for this hashtable.
        // If modifications made to the hashtable while an enumeration is
        // in progress, the MoveNext and Current methods of the
        // enumerator will throw an exception.
        //

        public virtual HashtableEntryEnumerator GetEnumerator()
        {
            return new HashtableEntryEnumerator(this);
        }

        // Internal method to get the hash code for an Object.  This will call
        // GetHashCode() on each object if you haven't provided an IHashCodeProvider
        // instance.  Otherwise, it calls hcp.GetHashCode(obj).
        protected virtual int GetHash(ReadOnlySpan<char> key)
        {
            var code = new HashCode();
            foreach (var c in key)
                code.Add(c);
            return code.ToHashCode();
        }

        // Is this Hashtable read-only?
        public virtual bool IsReadOnly => false;

        public virtual bool IsFixedSize => false;

        // Is this Hashtable synchronized?  See SyncRoot property
        public virtual bool IsSynchronized => false;

        // Internal method to compare two keys.  If you have provided an IComparer
        // instance in the constructor, this method will call comparer.Compare(item, key).
        // Otherwise, it will call item.Equals(key).
        //
        protected virtual bool KeyEquals(object? item, ReadOnlySpan<char> key)
        {
            if (item is string s)
                return s.AsSpan().SequenceEqual(key);
            return false;
        }

        // Returns a collection representing the keys of this hashtable. The order
        // in which the returned collection represents the keys is unspecified, but
        // it is guaranteed to be          buckets = newBuckets; the same order in which a collection returned by
        // GetValues represents the values of the hashtable.
        //
        // The returned collection is live in the sense that any changes
        // to the hash table are reflected in this collection.  It is not
        // a static copy of all the keys in the hash table.
        //
        public virtual KeyCollection Keys => _keys ??= new KeyCollection(this);

        // Returns a collection representing the values of this hashtable. The
        // order in which the returned collection represents the values is
        // unspecified, but it is guaranteed to be the same order in which a
        // collection returned by GetKeys represents the keys of the
        // hashtable.
        //
        // The returned collection is live in the sense that any changes
        // to the hash table are reflected in this collection.  It is not
        // a static copy of all the keys in the hash table.
        //
        public virtual ValueCollection Values => _values ??= new ValueCollection(this);

        // Inserts an entry into this hashtable. This method is called from the Set
        // and Add methods. If the add parameter is true and the given key already
        // exists in the hashtable, an exception is thrown.
        //private void Insert(ReadOnlySpan<char> key, T nvalue, bool add) => Insert(key.ToString(), nvalue, add);
        private void Insert(ReadOnlySpan<char> key, T nvalue, bool add)
        {
            if (_count >= _loadsize)
            {
                expand();
            }
            else if (_occupancy > _loadsize && _count > 100)
            {
                rehash();
            }

            // Assume we only have one thread writing concurrently.  Modify
            // buckets to contain new data, as long as we insert in the right order.
            uint hashcode = InitHash(key, _buckets.Length, out uint seed, out uint incr);
            int ntry = 0;
            int emptySlotNumber = -1; // We use the empty slot number to cache the first empty slot. We chose to reuse slots
            // create by remove that have the collision bit set over using up new slots.
            int bucketNumber = (int)(seed % (uint)_buckets.Length);
            do
            {
                // Set emptySlot number to current bucket if it is the first available bucket that we have seen
                // that once contained an entry and also has had a collision.
                // We need to search this entire collision chain because we have to ensure that there are no
                // duplicate entries in the table.
                if (emptySlotNumber == -1 && _buckets[bucketNumber].key == _buckets && _buckets[bucketNumber].hash_coll < 0)// (((buckets[bucketNumber].hash_coll & unchecked(0x80000000))!=0)))
                    emptySlotNumber = bucketNumber;

                // Insert the key/value pair into this bucket if this bucket is empty and has never contained an entry
                // OR
                // This bucket once contained an entry but there has never been a collision
                if (_buckets[bucketNumber].key == null ||
                    _buckets[bucketNumber].key == _buckets && (_buckets[bucketNumber].hash_coll & unchecked(0x80000000)) == 0)
                {
                    // If we have found an available bucket that has never had a collision, but we've seen an available
                    // bucket in the past that has the collision bit set, use the previous bucket instead
                    if (emptySlotNumber != -1) // Reuse slot
                        bucketNumber = emptySlotNumber;

                    // We pretty much have to insert in this order.  Don't set hash
                    // code until the value & key are set appropriately.
                    _isWriterInProgress = true;
                    _buckets[bucketNumber].val = nvalue;
                    _buckets[bucketNumber].key = key.ToString();
                    _buckets[bucketNumber].hash_coll |= (int)hashcode;
                    _count++;
                    UpdateVersion();
                    _isWriterInProgress = false;

                    return;
                }

                // The current bucket is in use
                // OR
                // it is available, but has had the collision bit set and we have already found an available bucket
                if ((_buckets[bucketNumber].hash_coll & 0x7FFFFFFF) == hashcode &&
                    KeyEquals(_buckets[bucketNumber].key, key))
                {
                    if (add)
                    {
                        throw new ArgumentException($"Adding Duplicate {key.ToString()} original was {_buckets[bucketNumber].key}");
                    }
                    _isWriterInProgress = true;
                    _buckets[bucketNumber].val = nvalue;
                    UpdateVersion();
                    _isWriterInProgress = false;

                    return;
                }

                // The current bucket is full, and we have therefore collided.  We need to set the collision bit
                // unless we have remembered an available slot previously.
                if (emptySlotNumber == -1)
                {// We don't need to set the collision bit here since we already have an empty slot
                    if (_buckets[bucketNumber].hash_coll >= 0)
                    {
                        _buckets[bucketNumber].hash_coll |= unchecked((int)0x80000000);
                        _occupancy++;
                    }
                }

                bucketNumber = (int)((bucketNumber + incr) % (uint)_buckets.Length);
            } while (++ntry < _buckets.Length);

            // This code is here if and only if there were no buckets without a collision bit set in the entire table
            if (emptySlotNumber != -1)
            {
                // We pretty much have to insert in this order.  Don't set hash
                // code until the value & key are set appropriately.
                _isWriterInProgress = true;
                _buckets[emptySlotNumber].val = nvalue;
                _buckets[emptySlotNumber].key = key.ToString();
                _buckets[emptySlotNumber].hash_coll |= (int)hashcode;
                _count++;
                UpdateVersion();
                _isWriterInProgress = false;

                return;
            }

            // If you see this assert, make sure load factor & count are reasonable.
            // Then verify that our double hash function (h2, described at top of file)
            // meets the requirements described above. You should never see this assert.
            Debug.Fail("hash table insert failed!  Load factor too high, or our double hashing function is incorrect.");
            throw new InvalidOperationException("Hash insert failed");
        }

        private void putEntry(bucket[] newBuckets, object key, T nvalue, int hashcode)
        {
            Debug.Assert(hashcode >= 0, "hashcode >= 0");  // make sure collision bit (sign bit) wasn't set.

            uint seed = (uint)hashcode;
            uint incr = unchecked(1 + seed * HashHelpers.HashPrime % ((uint)newBuckets.Length - 1));
            int bucketNumber = (int)(seed % (uint)newBuckets.Length);
            while (true)
            {
                if (newBuckets[bucketNumber].key == null || newBuckets[bucketNumber].key == _buckets)
                {
                    newBuckets[bucketNumber].val = nvalue;
                    newBuckets[bucketNumber].key = key;
                    newBuckets[bucketNumber].hash_coll |= hashcode;
                    return;
                }

                if (newBuckets[bucketNumber].hash_coll >= 0)
                {
                    newBuckets[bucketNumber].hash_coll |= unchecked((int)0x80000000);
                    _occupancy++;
                }
                bucketNumber = (int)((bucketNumber + incr) % (uint)newBuckets.Length);
            }
        }

        // Removes an entry from this hashtable. If an entry with the specified
        // key exists in the hashtable, it is removed. An ArgumentException is
        // thrown if the key is null.
        //
        public virtual bool Remove(ReadOnlySpan<char> key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            Debug.Assert(!_isWriterInProgress, "Race condition detected in usages of Hashtable - multiple threads appear to be writing to a Hashtable instance simultaneously!  Don't do that - use Hashtable.Synchronized.");

            // Assuming only one concurrent writer, write directly into buckets.
            uint hashcode = InitHash(key, _buckets.Length, out uint seed, out uint incr);
            int ntry = 0;

            bucket b;
            int bn = (int)(seed % (uint)_buckets.Length);  // bucketNumber
            do
            {
                b = _buckets[bn];
                if ((b.hash_coll & 0x7FFFFFFF) == hashcode &&
                    KeyEquals(b.key, key))
                {
                    _isWriterInProgress = true;
                    // Clear hash_coll field, then key, then value
                    _buckets[bn].hash_coll &= unchecked((int)0x80000000);
                    if (_buckets[bn].hash_coll != 0)
                    {
                        _buckets[bn].key = _buckets;
                    }
                    else
                    {
                        _buckets[bn].key = null;
                    }
                    _buckets[bn].val = default;  // Free object references sooner & simplify ContainsValue.
                    _count--;
                    UpdateVersion();
                    _isWriterInProgress = false;
                    return true;
                }
                bn = (int)((bn + incr) % (uint)_buckets.Length);
            } while (b.hash_coll < 0 && ++ntry < _buckets.Length);
            return false;
        }

        // Returns the object to synchronize on for this hash table.
        public virtual object SyncRoot => this;

        // Returns the number of associations in this hashtable.
        //
        public virtual int Count => _count;

        ICollection<string> IDictionary<string, T>.Keys => Keys as KeyCollection;

        ICollection<T> IDictionary<string, T>.Values => Values as ValueCollection;

        public T this[string key] { get => this[key.AsSpan()]; set => this[key.AsSpan()] = value; }

        // Returns a thread-safe wrapper for a Hashtable.
        //
        public static StringKeyDictionary<T> Synchronized(StringKeyDictionary<T> table)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));
            return new SyncHashtable(table);
        }

        public bool ContainsKey(string key)
        {
            return ContainsKey(key.AsSpan());
        }

        public bool Remove(string key)
        {
            return Remove(key.AsSpan());
        }

        public bool TryGetValue(string key, [MaybeNullWhen(false)] out T value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return TryGetValue(key.AsSpan(), out value);
        }
        public bool TryGetValue(ReadOnlySpan<char> key, [MaybeNullWhen(false)] out T value)
        {
            // Take a snapshot of buckets, in case another thread does a resize
            bucket[] lbuckets = _buckets;
            uint hashcode = InitHash(key, lbuckets.Length, out uint seed, out uint incr);
            int ntry = 0;

            bucket b;
            int bucketNumber = (int)(seed % (uint)lbuckets.Length);
            do
            {
                int currentversion;

                // A read operation on hashtable has three steps:
                //        (1) calculate the hash and find the slot number.
                //        (2) compare the hashcode, if equal, go to step 3. Otherwise end.
                //        (3) compare the key, if equal, go to step 4. Otherwise end.
                //        (4) return the value contained in the bucket.
                //     After step 3 and before step 4. A writer can kick in a remove the old item and add a new one
                //     in the same bucket. So in the reader we need to check if the hash table is modified during above steps.
                //
                // Writers (Insert, Remove, Clear) will set 'isWriterInProgress' flag before it starts modifying
                // the hashtable and will clear the flag when it is done.  When the flag is cleared, the 'version'
                // will be increased.  We will repeat the reading if a writer is in progress or done with the modification
                // during the read.
                //
                // Our memory model guarantee if we pick up the change in bucket from another processor,
                // we will see the 'isWriterProgress' flag to be true or 'version' is changed in the reader.
                //
                SpinWait spin = default;
                while (true)
                {
                    // this is volatile read, following memory accesses can not be moved ahead of it.
                    currentversion = _version;
                    b = lbuckets[bucketNumber];

                    if (!_isWriterInProgress && currentversion == _version)
                        break;

                    spin.SpinOnce();
                }

                if (b.key == null)
                {
                    value = default;
                    return false;
                }
                if ((b.hash_coll & 0x7FFFFFFF) == hashcode &&
                    KeyEquals(b.key, key))
                {
                    Debug.Assert(b.val != null);
                    value = b.val;
                    return true;
                }
                bucketNumber = (int)((bucketNumber + incr) % (uint)lbuckets.Length);
            } while (b.hash_coll < 0 && ++ntry < lbuckets.Length);
            value = default;
            return false;
        }

        public void Add(KeyValuePair<string, T> item)
        {
            Add(item.Key.AsSpan(), item.Value);
        }

        public bool Contains(KeyValuePair<string, T> item)
        {
            if (!TryGetValue(item.Key, out var b))
                return false;
            return Equals(item.Value, b);
        }

        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
        {
            CopyEntries(array, arrayIndex); ;
        }

        public bool Remove(KeyValuePair<string, T> item)
        {
            if (!Contains(item))
                return false;
            return Remove(item.Key);
        }

        IEnumerator<KeyValuePair<string, T>> IEnumerable<KeyValuePair<string, T>>.GetEnumerator()
        {
            return GetEnumerator();
        }

        void IDictionary<string, T>.Add(string key, T value)
        {
            Add(key.AsSpan(), value);
        }



        // Implements a Collection for the keys of a hashtable. An instance of this
        // class is created by the GetKeys method of a hashtable.
        public sealed class KeyCollection : IReadOnlyCollection<string>, ICollection, ICollection<string>
        {
            private readonly StringKeyDictionary<T> _hashtable;

            internal KeyCollection(StringKeyDictionary<T> hashtable)
            {
                _hashtable = hashtable;
            }

            public void CopyTo(Array array, int arrayIndex)
            {
                if (array == null)
                    throw new ArgumentNullException(nameof(array));
                if (array.Rank != 1)
                    throw new ArgumentException("Multirank array not supported", nameof(array));
                if (arrayIndex < 0)
                    throw new ArgumentOutOfRangeException(nameof(arrayIndex), "Non negativ index");
                if (array.Length - arrayIndex < _hashtable._count)
                    throw new ArgumentException("Array to small");
                _hashtable.CopyKeys(array, arrayIndex);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public IEnumerator<string> GetEnumerator()
            {
                return new HashtableKeyEnumerator(_hashtable);
            }

            void ICollection<string>.Add(string item)
            {
                throw new NotSupportedException();
            }

            void ICollection<string>.Clear()
            {
                throw new NotSupportedException();
            }

            bool ICollection<string>.Contains(string item)
            {
                return _hashtable.ContainsKey(item);
            }

            void ICollection<string>.CopyTo(string[] array, int arrayIndex)
            {
                _hashtable.CopyKeys(array, arrayIndex);
            }

            bool ICollection<string>.Remove(string item)
            {
                throw new NotSupportedException();
            }

            public bool IsSynchronized => _hashtable.IsSynchronized;

            public object SyncRoot => _hashtable.SyncRoot;

            public int Count => _hashtable._count;

            bool ICollection<string>.IsReadOnly => true;
        }

        // Implements a Collection for the values of a hashtable. An instance of
        // this class is created by the GetValues method of a hashtable.
        public sealed class ValueCollection : ICollection, IReadOnlyCollection<T>, ICollection<T>
        {
            private readonly StringKeyDictionary<T> _hashtable;

            internal ValueCollection(StringKeyDictionary<T> hashtable)
            {
                _hashtable = hashtable;
            }

            public void CopyTo(Array array, int arrayIndex)
            {
                if (array == null)
                    throw new ArgumentNullException(nameof(array));
                if (array.Rank != 1)
                    throw new ArgumentException("Multidimensional array not supported.", nameof(array));
                if (arrayIndex < 0)
                    throw new ArgumentOutOfRangeException(nameof(arrayIndex), "non negativ index.");
                if (array.Length - arrayIndex < _hashtable._count)
                    throw new ArgumentException("Array to small");
                _hashtable.CopyValues(array, arrayIndex);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public IEnumerator<T> GetEnumerator()
            {
                return new HashtableValueEnumerator(_hashtable);
            }

            void ICollection<T>.Add(T item)
            {
                throw new NotSupportedException();
            }

            void ICollection<T>.Clear()
            {
                throw new NotSupportedException();
            }

            bool ICollection<T>.Contains(T item)
            {
                return _hashtable.ContainsValue(item);
            }

            void ICollection<T>.CopyTo(T[] array, int arrayIndex)
            {
                _hashtable.CopyValues(array, arrayIndex);
            }

            bool ICollection<T>.Remove(T item)
            {
                throw new NotSupportedException();
            }

            public bool IsSynchronized => _hashtable.IsSynchronized;

            public object SyncRoot => _hashtable.SyncRoot;

            public int Count => _hashtable._count;

            bool ICollection<T>.IsReadOnly => true;
        }

        // Synchronized wrapper for hashtable
        private sealed class SyncHashtable : StringKeyDictionary<T>, IEnumerable
        {
            private StringKeyDictionary<T> _table;

            internal SyncHashtable(StringKeyDictionary<T> table) : base(false)
            {
                _table = table;
            }



            public override int Count => _table.Count;

            public override bool IsReadOnly => _table.IsReadOnly;

            public override bool IsFixedSize => _table.IsFixedSize;

            public override bool IsSynchronized => true;

            public override T this[ReadOnlySpan<char> key]
            {
                get => _table[key];
                set
                {
                    lock (_table.SyncRoot)
                    {
                        _table[key] = value;
                    }
                }
            }

            public override object SyncRoot => _table.SyncRoot;

            public override void Add(ReadOnlySpan<char> key, T value)
            {
                lock (_table.SyncRoot)
                {
                    _table.Add(key, value);
                }
            }

            public override void Clear()
            {
                lock (_table.SyncRoot)
                {
                    _table.Clear();
                }
            }

            public override bool ContainsKey(ReadOnlySpan<char> key)
            {
                return _table.ContainsKey(key);
            }

            public override bool ContainsValue(T key)
            {
                lock (_table.SyncRoot)
                {
                    return _table.ContainsValue(key);
                }
            }

            public override void CopyTo(Array array, int arrayIndex)
            {
                lock (_table.SyncRoot)
                {
                    _table.CopyTo(array, arrayIndex);
                }
            }

            public override object Clone()
            {
                lock (_table.SyncRoot)
                {
                    return Hashtable.Synchronized((Hashtable)_table.Clone());
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return _table.GetEnumerator();
            }

            public override HashtableEntryEnumerator GetEnumerator()
            {
                return _table.GetEnumerator();
            }



            public override KeyCollection Keys
            {
                get
                {
                    lock (_table.SyncRoot)
                    {
                        return _table.Keys;
                    }
                }
            }

            public override ValueCollection Values
            {
                get
                {
                    lock (_table.SyncRoot)
                    {
                        return _table.Values;
                    }
                }
            }

            public override bool Remove(ReadOnlySpan<char> key)
            {
                lock (_table.SyncRoot)
                {
                    return _table.Remove(key);
                }
            }


            //internal override KeyValuePairs[] ToKeyValuePairsArray()
            //{
            //    return _table.ToKeyValuePairsArray();
            //}
        }

        public abstract class HashtableEnumeratorBase<TReturn> : ICloneable, IEnumerator<TReturn>
        {
            private readonly StringKeyDictionary<T> _hashtable;
            private readonly int _version;
            private int _bucket;
            private bool _current;
            private string? _currentKey;
            private T? _currentValue;

            private protected HashtableEnumeratorBase(StringKeyDictionary<T> hashtable)
            {
                _hashtable = hashtable;
                _bucket = hashtable._buckets.Length;
                _version = hashtable._version;
                _current = false;
            }


            public string Key
            {
                get
                {
                    if (!_current)
                        throw new InvalidOperationException("Enumeration not started");
                    Debug.Assert(_currentKey != null);
                    return _currentKey;
                }
            }

            public KeyValuePair<string, T> Entry
            {
                get
                {
                    if (!_current)
                        throw new InvalidOperationException("OP can't happen");

                    Debug.Assert(_currentValue != null);
                    Debug.Assert(_currentKey != null);
                    return KeyValuePair.Create(_currentKey, _currentValue);
                }
            }

            public T Value
            {
                get
                {
                    if (!_current)
                        throw new InvalidOperationException("Enum OP Can't happen");
                    Debug.Assert(_currentValue != null);
                    return _currentValue;
                }
            }

            public abstract TReturn Current { get; }


            object? IEnumerator.Current =>  this.Current;

            public object Clone() => MemberwiseClone();

            public void Dispose()
            {
                GC.SuppressFinalize(this);
            }

            public bool MoveNext()
            {
                if (_version != _hashtable._version)
                    throw new InvalidOperationException("Failed Version");
                while (_bucket > 0)
                {
                    _bucket--;
                    object? keyv = _hashtable._buckets[_bucket].key;
                    if (keyv is string s)
                    {
                        _currentKey = s;
                        _currentValue = _hashtable._buckets[_bucket].val;
                        _current = true;
                        return true;
                    }
                }
                _current = false;
                return false;
            }

            public void Reset()
            {
                if (_version != _hashtable._version)
                    throw new InvalidOperationException("Failed Version");
                _current = false;
                _bucket = _hashtable._buckets.Length;
                _currentKey = null;
                _currentValue = default;
            }
        }

        // Implements an enumerator for a hashtable. The enumerator uses the
        // internal version number of the hashtable to ensure that no modifications
        // are made to the hashtable while an enumeration is in progress.
        public sealed class HashtableValueEnumerator : HashtableEnumeratorBase<T>
        {
            internal HashtableValueEnumerator(StringKeyDictionary<T> hashtable) : base(hashtable) { }

            public override T Current => Value;
        }
        public sealed class HashtableKeyEnumerator : HashtableEnumeratorBase<string>
        {
            internal HashtableKeyEnumerator(StringKeyDictionary<T> hashtable) : base(hashtable) { }

            public override string Current => Key;
        }
        public sealed class HashtableEntryEnumerator : HashtableEnumeratorBase<KeyValuePair<string, T>>, IDictionaryEnumerator
        {
            internal HashtableEntryEnumerator(StringKeyDictionary<T> hashtable) : base(hashtable) { }

            public override KeyValuePair<string, T> Current => Entry;

            object IDictionaryEnumerator.Key => Key;

            object? IDictionaryEnumerator.Value => Value;

            DictionaryEntry IDictionaryEnumerator.Entry => new(Entry.Key, Entry.Value);
        }

        // internal debug view class for hashtable
        internal sealed class HashtableDebugView
        {
            private readonly StringKeyDictionary<T> _hashtable;

            public HashtableDebugView(StringKeyDictionary<T> hashtable)
            {
                if (hashtable == null)
                {
                    throw new ArgumentNullException(nameof(hashtable));
                }

                _hashtable = hashtable;
            }

            [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
            public KeyValuePairs[] Items => _hashtable.ToKeyValuePairsArray();
        }

        private static partial class HashHelpers
        {
            public const uint HashCollisionThreshold = 100;

            // This is the maximum prime smaller than Array.MaxLength.
            public const int MaxPrimeArrayLength = 0x7FFFFFC3;

            public const int HashPrime = 101;

            // Table of prime numbers to use as hash table sizes.
            // A typical resize algorithm would pick the smallest prime number in this array
            // that is larger than twice the previous capacity.
            // Suppose our Hashtable currently has capacity x and enough elements are added
            // such that a resize needs to occur. Resizing first computes 2x then finds the
            // first prime in the table greater than 2x, i.e. if primes are ordered
            // p_1, p_2, ..., p_i, ..., it finds p_n such that p_n-1 < 2x < p_n.
            // Doubling is important for preserving the asymptotic complexity of the
            // hashtable operations such as add.  Having a prime guarantees that double
            // hashing does not lead to infinite loops.  IE, your hash function will be
            // h1(key) + i*h2(key), 0 <= i < size.  h2 and the size must be relatively prime.
            // We prefer the low computation costs of higher prime numbers over the increased
            // memory allocation of a fixed prime number i.e. when right sizing a HashSet.
            private static readonly int[] s_primes =
            {
            3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
            1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
            17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
            187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
            1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
        };

            public static bool IsPrime(int candidate)
            {
                if ((candidate & 1) != 0)
                {
                    int limit = (int)Math.Sqrt(candidate);
                    for (int divisor = 3; divisor <= limit; divisor += 2)
                    {
                        if (candidate % divisor == 0)
                            return false;
                    }
                    return true;
                }
                return candidate == 2;
            }

            public static int GetPrime(int min)
            {
                if (min < 0)
                    throw new ArgumentException("Capathyt Overflow");

                foreach (int prime in s_primes)
                {
                    if (prime >= min)
                        return prime;
                }

                // Outside of our predefined table. Compute the hard way.
                for (int i = min | 1; i < int.MaxValue; i += 2)
                {
                    if (IsPrime(i) && (i - 1) % HashPrime != 0)
                        return i;
                }
                return min;
            }

            // Returns size of hashtable to grow to.
            public static int ExpandPrime(int oldSize)
            {
                int newSize = 2 * oldSize;

                // Allow the hashtables to grow to maximum possible size (~2G elements) before encountering capacity overflow.
                // Note that this check works even when _items.Length overflowed thanks to the (uint) cast
                if ((uint)newSize > MaxPrimeArrayLength && MaxPrimeArrayLength > oldSize)
                {
                    Debug.Assert(MaxPrimeArrayLength == GetPrime(MaxPrimeArrayLength), "Invalid MaxPrimeArrayLength");
                    return MaxPrimeArrayLength;
                }

                return GetPrime(newSize);
            }

            /// <summary>Returns approximate reciprocal of the divisor: ceil(2**64 / divisor).</summary>
            /// <remarks>This should only be used on 64-bit.</remarks>
            public static ulong GetFastModMultiplier(uint divisor) =>
                ulong.MaxValue / divisor + 1;

            /// <summary>Performs a mod operation using the multiplier pre-computed with <see cref="GetFastModMultiplier"/>.</summary>
            /// <remarks>This should only be used on 64-bit.</remarks>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint FastMod(uint value, uint divisor, ulong multiplier)
            {
                // We use modified Daniel Lemire's fastmod algorithm (https://github.com/dotnet/runtime/pull/406),
                // which allows to avoid the long multiplication if the divisor is less than 2**31.
                Debug.Assert(divisor <= int.MaxValue);

                // This is equivalent of (uint)Math.BigMul(multiplier * value, divisor, out _). This version
                // is faster than BigMul currently because we only need the high bits.
                uint highbits = (uint)(((multiplier * value >> 32) + 1) * divisor >> 32);

                Debug.Assert(highbits == value % divisor);
                return highbits;
            }
        }
        [DebuggerDisplay("{_value}", Name = "[{_key}]")]
        internal sealed class KeyValuePairs
        {
            [DebuggerBrowsable(DebuggerBrowsableState.Never)]
            private readonly object _key;

            [DebuggerBrowsable(DebuggerBrowsableState.Never)]
            private readonly object? _value;

            public KeyValuePairs(object key, object? value)
            {
                _value = value;
                _key = key;
            }
        }
    }

}